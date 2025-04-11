package com.mycodefu;

import com.mongodb.client.ListSearchIndexesIterable;
import com.mycodefu.PersonDataAccess.Person;
import com.mycodefu.atlassearch.util.IndexValidator;
import com.mycodefu.atlassearch.util.IndexValidator.IndexValidationResults;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.ExecConfig;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBAtlasLocalContainer;
import org.testcontainers.shaded.com.google.common.io.Resources;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.testcontainers.shaded.org.apache.commons.lang3.ArrayUtils.toArray;

@Testcontainers
class PersonDataAccessSearchTest {

    @Container
    private static final MongoDBAtlasLocalContainer mongoDBContainer = new MongoDBAtlasLocalContainer("mongodb/mongodb-atlas-local:8.0.5")
            .withClasspathResourceMapping(
                    "/seed-data",
                    "/tmp/seed-data",
                    BindMode.READ_WRITE
            );
    @AutoClose
    private static PersonDataAccess personDataAccess;

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        System.out.println("Initializing data access with MongoDB connection string: " + mongoDBContainer.getConnectionString());
        personDataAccess = new PersonDataAccess(mongoDBContainer.getConnectionString());

        Instant startSeedDataRestore = Instant.now();
        mongoDBContainer.execInContainer(ExecConfig.builder()
                .workDir("/tmp/seed-data")
                .command(toArray("mongorestore", "--gzip"))
                .build());
        System.out.println("Loading seed data took: " + Instant.now().minusMillis(startSeedDataRestore.toEpochMilli()).toEpochMilli() + "ms");

        Instant startIndex = Instant.now();
        String personSearchMappings = Resources.toString(Resources.getResource("atlas-search-indexes/examples/person/person_search.json"), UTF_8);
        personDataAccess.collection.createSearchIndex("person_search", BsonDocument.parse(personSearchMappings));
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> {
                    ListSearchIndexesIterable<Document> searchIndexes = personDataAccess.collection.listSearchIndexes();
                    Document personIndex = searchIndexes.into(new ArrayList<>()).stream().filter(index -> index.getString("name").equals("person_search")).findFirst().orElseThrow();
                    return personIndex.getString("status").equals("READY");
                });
        System.out.printf("Index created and ready in %dms%n", Duration.between(startIndex, Instant.now()).toMillis());
    }

    @Test
    void shouldFindPersonByBioWord_dedicated() {
        // Given
        String word = "dedicated";

        // When
        List<Person> dedicatedPeople = personDataAccess.findPersonByBio(word, false);

        // Then
        assertEquals(50, dedicatedPeople.size());
        assertTrue(dedicatedPeople.stream().allMatch(person -> person.bio().contains(word)));
    }

    @Test
    void shouldFindPersonByBioWord_fuzzy_yesr() {
        // Given year (with a typo)
        String word = "yesr";

        // When fuzzy searched
        List<Person> yearPeople = personDataAccess.findPersonByBio(word, true);

        // Then match bios with 'year', or 'years'
        assertEquals(50, yearPeople.size());
        assertTrue(yearPeople.stream().allMatch(person -> person.bio().contains("year")));

        //find the surrounding words and print them
        yearPeople.forEach(person -> {
            String bio = person.bio();
            int index = bio.indexOf("year");
            int start = Math.max(0, index - 20);
            int end = Math.min(bio.length(), index + word.length() + 20);
            String surroundingYear = bio.substring(start, end);
            System.out.println(surroundingYear);
        });
    }

    @Test
    void checkIndexValidation() {
        // Given
        String indexName = "person_search";

        // When
        IndexValidationResults validationResults = IndexValidator.validateIndexes(personDataAccess.collection, indexName);
        validationResults.printResults();

        // Then
        assertTrue(validationResults.valid());
    }
}