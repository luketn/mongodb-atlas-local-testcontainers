package com.mycodefu;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertOneResult;
import org.bson.BsonType;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonRepresentation;
import org.bson.types.ObjectId;

import static com.mongodb.client.model.Filters.eq;

public class PersonDataAccess implements AutoCloseable {
    private final MongoClient mongoClient;
    private final MongoCollection<Person> collection;

    public record Person(
            @BsonId
            @BsonRepresentation(BsonType.OBJECT_ID)
            String id,
            String name,
            int age,
            String bio
    ) { }

    public PersonDataAccess(String connectionString) {
        this.mongoClient = MongoClients.create(connectionString);
        this.collection = this.mongoClient.getDatabase("examples").getCollection("person", Person.class);
    }

    public String insertPerson(Person person) {
        InsertOneResult insertOneResult = this.collection.insertOne(person);
        return insertOneResult.getInsertedId().asObjectId().getValue().toHexString();
    }

    public Person getPerson(String id) {
        return this.collection.find(eq("_id", new ObjectId(id))).first();
    }

    public void updatePerson(Person person) {
        this.collection.replaceOne(eq("_id", new ObjectId(person.id())), person);
    }

    public void deletePerson(String id) {
        this.collection.deleteOne(eq("_id", new ObjectId(id)));
    }

    @Override
    public void close() {
        this.mongoClient.close();
    }
}
