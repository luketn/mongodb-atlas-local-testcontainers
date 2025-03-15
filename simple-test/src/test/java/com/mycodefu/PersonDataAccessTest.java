package com.mycodefu;

import com.mycodefu.PersonDataAccess.Person;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBAtlasLocalContainer;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PersonDataAccessTest {

    @Container
    private static final MongoDBAtlasLocalContainer mongoDBContainer = new MongoDBAtlasLocalContainer("mongodb/mongodb-atlas-local:8.0.5");
    @AutoClose
    private static PersonDataAccess personDataAccess;

    @BeforeAll
    static void beforeAll() {
        personDataAccess = new PersonDataAccess(mongoDBContainer.getConnectionString());
    }

    @Test
    void shouldInsertAndRetrievePerson() {
        // Given
        Person person = new Person(
                null,
                "John Doe",
                30,
                "Software Developer"
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
        assertEquals("Software Developer", retrievedPerson.bio());
    }

    @Test
    void shouldUpdatePerson() {
        // Given
        Person person = new Person(
                null,
                "Jane Smith",
                25,
                "Data Scientist"
        );
        String id = personDataAccess.insertPerson(person);

        // When
        Person updatedPerson = new Person(
                id,
                "Jane Smith",
                26,
                "Senior Data Scientist"
        );
        personDataAccess.updatePerson(updatedPerson);
        Person retrievedPerson = personDataAccess.getPerson(id);

        // Then
        assertEquals(26, retrievedPerson.age());
        assertEquals("Senior Data Scientist", retrievedPerson.bio());
    }

    @Test
    void shouldDeletePerson() {
        // Given
        Person person = new Person(
                null,
                "Bob Johnson",
                40,
                "Manager"
        );
        String id = personDataAccess.insertPerson(person);

        // When
        personDataAccess.deletePerson(id);
        Person retrievedPerson = personDataAccess.getPerson(id);

        // Then
        assertNull(retrievedPerson);
    }
}