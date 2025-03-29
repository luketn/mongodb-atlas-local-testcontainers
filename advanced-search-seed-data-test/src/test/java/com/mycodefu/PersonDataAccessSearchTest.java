package com.mycodefu;

import com.mycodefu.PersonDataAccess.Person;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.ExecConfig;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBAtlasLocalContainer;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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
        Instant start = Instant.now();
        mongoDBContainer.execInContainer(ExecConfig.builder()
                .workDir("/tmp/seed-data")
                .command(new String[]{"bash", "seed-data.sh"})
                .build());
        System.out.println("Loading seed data took: " + Instant.now().minusMillis(start.toEpochMilli()).toEpochMilli() + "ms");

        System.out.println("Initializing data access with MongoDB connection string: " + mongoDBContainer.getConnectionString());
        personDataAccess = new PersonDataAccess(mongoDBContainer.getConnectionString());
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
}