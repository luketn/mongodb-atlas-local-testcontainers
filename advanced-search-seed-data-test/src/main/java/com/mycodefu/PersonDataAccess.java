package com.mycodefu;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.search.*;
import com.mongodb.client.result.InsertOneResult;
import org.bson.BsonType;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonRepresentation;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.search.SearchPath.fieldPath;

public class PersonDataAccess implements AutoCloseable {
    static Logger log = LoggerFactory.getLogger(PersonDataAccess.class);

    final MongoClient mongoClient;
    final MongoCollection<Person> collection;

    public record Person(
            @BsonId
            @BsonRepresentation(BsonType.OBJECT_ID)
            String id,
            String name,
            int age,
            String job,
            String bio
    ) {
        public static Person of(String name, int age, String job, String bio) {
            return new Person(null, name, age, job, bio);
        }
    }

    public PersonDataAccess(String connectionString) {
        this.mongoClient = MongoClients.create(connectionString);
        this.collection = this.mongoClient.getDatabase("examples").getCollection("person", Person.class);
    }

    public String insertPerson(Person person) {
        InsertOneResult insertOneResult = this.collection.insertOne(person);
        return Objects.requireNonNull(insertOneResult.getInsertedId()).asObjectId().getValue().toHexString();
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

    public List<Person> findPersonByBio(String query, boolean fuzzy) {
        //use Atlas Search to find a person by their bio
        TextSearchOperator bioOperator = SearchOperator.text(fieldPath("bio"), query);
        if (fuzzy) {
            bioOperator = bioOperator
                    .fuzzy(FuzzySearchOptions
                            .fuzzySearchOptions()
                            .maxEdits(2)
                            .prefixLength(2)
                            .maxExpansions(100)
                    );
        }
        List<Bson> aggregateStages = List.of(
                Aggregates.search(
                        bioOperator
                , SearchOptions.searchOptions().index("person_search")),
                Aggregates.limit(50)

        );

        if (log.isTraceEnabled()) {
            for (Bson aggregateStage : aggregateStages) {
                log.trace(aggregateStage.toBsonDocument().toJson(JsonWriterSettings.builder().indent(true).build()));
            }
        }

        ArrayList<Person> results = collection.aggregate(aggregateStages, Person.class).into(new ArrayList<>());

        if (log.isTraceEnabled()) {
            log.trace("Found {} results", results.size());
            if (!results.isEmpty()) {
                log.trace("First result: {}", results.getFirst());
            }
        }

        return results;
    }

    @Override
    public void close() {
        this.mongoClient.close();
    }
}
