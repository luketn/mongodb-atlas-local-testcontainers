package com.mycodefu;

import com.mycodefu.PersonDataAccess.Person;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBAtlasLocalContainer;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PersonDataAccessCRUDTest {

    @Container
    private static final MongoDBAtlasLocalContainer mongoDBContainer = new MongoDBAtlasLocalContainer("mongodb/mongodb-atlas-local:8.0.5");
    @AutoClose
    private static PersonDataAccess personDataAccess;

    @BeforeAll
    static void beforeAll() {
        System.out.println("Initializing data access with MongoDB connection string: " + mongoDBContainer.getConnectionString());
        personDataAccess = new PersonDataAccess(mongoDBContainer.getConnectionString());
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