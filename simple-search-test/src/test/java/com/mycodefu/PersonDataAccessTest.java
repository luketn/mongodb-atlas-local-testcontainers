package com.mycodefu;

import com.mongodb.client.ListSearchIndexesIterable;
import com.mycodefu.PersonDataAccess.Person;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBAtlasLocalContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PersonDataAccessTest {

    @Container
    private static final MongoDBAtlasLocalContainer mongoDBContainer = new MongoDBAtlasLocalContainer("mongodb/mongodb-atlas-local:8.0.5");
    @AutoClose
    private static PersonDataAccess personDataAccess;

    @BeforeAll
    static void beforeAll() {
        System.out.println("Initializing data access with MongoDB connection string: " + mongoDBContainer.getConnectionString());
        personDataAccess = new PersonDataAccess(mongoDBContainer.getConnectionString());

        //insert a few records for testing
        personDataAccess.insertPerson(Person.of("Miss Scotty Leffler", 32, "farmer", "At 32, Miss Scotty Leffler is a dedicated farmer known for her innovative approaches to sustainable agriculture on her family-owned farm. Passionate about environmental stewardship, she combines traditional farming methods with modern technology to enhance crop yield and soil health."));
        personDataAccess.insertPerson(Person.of("Raymon Wehner", 27, "dental hygienist", "At just 27 years old, Raymon Wehner is an accomplished dental hygienist dedicated to promoting oral health through comprehensive patient education and preventative care practices. With a passion for community outreach, Raymon frequently volunteers at local schools to teach children about the importance of maintaining good dental hygiene habits from an early age."));
        personDataAccess.insertPerson(Person.of("Miss Steve Rempel", 22, "businessman", "At just 22 years old, Miss Steve Rempel has already made a significant mark as an innovative entrepreneur with a keen eye for emerging market trends and opportunities. Her dynamic approach to business is characterized by her ability to adapt quickly and lead diverse teams towards achieving ambitious goals, establishing herself as a rising star in the entrepreneurial landscape."));
        personDataAccess.insertPerson(Person.of("Dustin Schinner", 45, "engineer", "At 45, Dustin Schinner is an accomplished engineer with over two decades of experience in innovative design and sustainable technology development. Known for his forward-thinking approach, he has led numerous successful projects that integrate cutting-edge solutions to address modern engineering challenges."));
        personDataAccess.insertPerson(Person.of("Eartha Mosciski", 39, "window cleaner", "At 39, Eartha Mosciski has mastered the art of window cleaning, transforming ordinary buildings into sparkling showcases with her meticulous touch and eye for detail. Beyond just clearing away grime, she sees each pane as a canvas where light is artistically framed, bringing clarity and brightness to every view."));
        personDataAccess.insertPerson(Person.of("Jackqueline Osinski", 23, "astronomer", "At just 23 years old, Jacqueline Osinski is making waves as an innovative astronomer dedicated to exploring the mysteries of distant galaxies. Her cutting-edge research on dark matter distribution has already earned her recognition in the scientific community and promises to reshape our understanding of the cosmos."));
        personDataAccess.insertPerson(Person.of("Richard Ortiz II", 55, "lecturer", "Richard Ortiz II, at 55, is an esteemed lecturer renowned for his engaging teaching style and profound knowledge in his field of expertise. With years of experience shaping the minds of students across various disciplines, he continues to inspire through innovative educational approaches and a passion for lifelong learning."));
        personDataAccess.insertPerson(Person.of("Brenton Bergstrom", 50, "bookkeeper", "At 50, Brenton Bergstrom is a seasoned bookkeeper with over two decades of experience ensuring the financial accuracy and integrity of businesses. Known for his meticulous attention to detail and dedication to precision, he plays a vital role in helping companies maintain their fiscal health and compliance."));
        personDataAccess.insertPerson(Person.of(
                "Carroll Ankunding",
                39,
                "travel agent",
                "At 39, Carroll Ankunding is an experienced travel agent who combines her passion for exploration with a knack for crafting unforgettable journeys for clients. With nearly two decades of industry experience, she excels in tailoring personalized travel experiences that cater to the unique desires and needs of each traveler."));

        personDataAccess.collection.createSearchIndex("person_search", BsonDocument.parse("""
                {
                   "mappings": {
                     "dynamic": false,
                     "fields": {
                       "name": {
                           "type": "string",
                           "analyzer": "lucene.standard"
                       },
                       "age": {
                         "type": "number",
                         "representation": "int64",
                         "indexDoubles": false
                       },
                       "job": [
                         {
                           "type": "token"
                         },
                         {
                           "type": "stringFacet"
                         }
                       ],
                       "bio": {
                           "type": "string",
                           "analyzer": "lucene.standard"
                       }
                     }
                   }
                 }
                """));

        Instant startTime = Instant.now();
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> {
                    ListSearchIndexesIterable<Document> searchIndexes = personDataAccess.collection.listSearchIndexes();
                    Document personIndex = searchIndexes.into(new ArrayList<>()).stream().filter(index -> index.getString("name").equals("person_search")).findFirst().orElseThrow();
                    return personIndex.getString("status").equals("READY");
                });
        System.out.printf("Index created and ready in %dms%n", Duration.between(startTime, Instant.now()).toMillis());
    }

    @Test
    void shouldFindPersonByBioWord_dedicated() {
        // Given
        String word = "dedicated";

        // When
        List<Person> dedicatedPeople = personDataAccess.findPersonByBio(word);

        // Then
        assertEquals(3, dedicatedPeople.size());
        assertTrue(dedicatedPeople.stream().allMatch(person -> person.bio().contains(word)));
    }

    @Test
    void shouldFindPersonByBioWord_fuzzy_yesr() {
        // Given year (with a typo)
        String word = "yesr";

        // When fuzzy searched
        List<Person> yearPeople = personDataAccess.findPersonByBio(word);

        // Then match bios with 'year', or 'years'
        assertEquals(4, yearPeople.size());
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
    void shouldInsertAndRetrievePerson() {
        // Given
        Person person = new Person(
                null,
                "John Doe",
                30,
                "Software Developer",
                "John is a software developer who loves to code."
        );

        // When
        String id = personDataAccess.insertPerson(person);
        Person retrievedPerson = personDataAccess.getPerson(id);

        // Then
        assertNotNull(id);
        assertNotNull(retrievedPerson);
        assertEquals(id, retrievedPerson.id());
        assertEquals("John Doe", retrievedPerson.name());
        assertEquals(30, retrievedPerson.age());
        assertEquals("Software Developer", retrievedPerson.job());
    }

    @Test
    void shouldUpdatePerson() {
        // Given
        Person person = new Person(
                null,
                "Jane Smith",
                25,
                "Data Scientist",
                "Jane is a data scientist who loves to analyze data."
        );
        String id = personDataAccess.insertPerson(person);

        // When
        Person updatedPerson = new Person(
                id,
                "Jane Smith",
                26,
                "Senior Data Scientist",
                "Jane is a senior data scientist who loves to analyze data."

        );
        personDataAccess.updatePerson(updatedPerson);
        Person retrievedPerson = personDataAccess.getPerson(id);

        // Then
        assertEquals(26, retrievedPerson.age());
        assertEquals("Senior Data Scientist", retrievedPerson.job());
    }

    @Test
    void shouldDeletePerson() {
        // Given
        Person person = new Person(
                null,
                "Bob Johnson",
                40,
                "Manager",
                "Bob is a manager who loves to manage. Where are those TPS reports?"
        );
        String id = personDataAccess.insertPerson(person);

        // When
        personDataAccess.deletePerson(id);
        Person retrievedPerson = personDataAccess.getPerson(id);

        // Then
        assertNull(retrievedPerson);
    }
}