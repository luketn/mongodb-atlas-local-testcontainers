package com.mycodefu.atlassearch.util;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;


public class IndexValidator {

    // Only check the following keys for equality, ignoring other keys which may be added by MongoDB internally.
    public static final List<String> check_equality_of_keys = List.of("type", "normalizer", "representation", "analyzer", "indexDoubles", "indexIntegers");

    public record InvalidField(String fieldParent, String fieldName, String message) { }
    public record IndexValidationResults(boolean valid, String message,  List<InvalidField> invalidFields) {
        public static IndexValidationResults error(String message) {return new IndexValidationResults(false, message, List.of());}
        public static IndexValidationResults success(String message) {return new IndexValidationResults(true, message, List.of());}
        public void printResults() {
            if (valid) {
                System.out.println("Index validation successful: " + message);
            } else {
                System.out.println("Index validation failed: " + message);
                for (InvalidField invalidField : invalidFields) {
                    String fieldPrefix = invalidField.fieldParent == null || invalidField.fieldParent.isEmpty() ? "" : invalidField.fieldParent + ".";
                    String fieldPath = fieldPrefix + invalidField.fieldName;
                    System.out.println("Invalid field: " + fieldPath + ": " + invalidField.message);
                }
            }
        }
    }

    public static <T> IndexValidationResults validateIndexes(MongoCollection<T> collection, String indexName) {
        String indexResource = readResourceAsString(
                "atlas-search-indexes/%s/%s/%s.json"
                        .formatted(
                                collection.getNamespace().getDatabaseName(),
                                collection.getNamespace().getCollectionName(),
                                indexName
                        )
        );
        Document expectedIndex = Document.parse(indexResource);

        Optional<Document> actualIndexDocument = collection.listSearchIndexes().into(new ArrayList<>()).stream()
                .filter(index -> index.getString("name").equals(indexName))
                .findFirst();

        if (actualIndexDocument.isEmpty()) {
            return IndexValidationResults.error("Index not found: " + indexName);
        } else {
            if (!actualIndexDocument.get().getString("status").equals("READY")) {
                return IndexValidationResults.error("Index is not ready: " + indexName);
            }
            Document actualIndex = actualIndexDocument.get().get("latestDefinition", Document.class);
            return compareAtlasSearchIndexMapping(
                    expectedIndex.get("mappings", Document.class),
                    actualIndex.get("mappings", Document.class),
                    new ArrayList<>()
            );
        }
    }

    /**
     * Check if the given atlas mappings matches the expected mappings.
     * (recursively)
     */
    public static IndexValidationResults compareAtlasSearchIndexMapping(Document expectedMapping, Document actualMapping, List<InvalidField> invalidFieldsSoFar) {
        if (expectedMapping == null || actualMapping == null) {
            return IndexValidationResults.error("Expected or actual mappings are null");
        }
        if (!actualMapping.containsKey("dynamic")) {
            return IndexValidationResults.error("Actual mappings do not contain 'dynamic' key");
        }

        if (expectedMapping.getBoolean("dynamic") != actualMapping.getBoolean("dynamic")) {
            return IndexValidationResults.error("Dynamic mapping settings do not match");
        }
        if (expectedMapping.containsKey("fields") && actualMapping.containsKey("fields")) {
            Document expectedFields = expectedMapping.get("fields", Document.class);
            Document actualFields = actualMapping.get("fields", Document.class);
            compareFields(expectedFields, actualFields, "", invalidFieldsSoFar);
        }
        if (invalidFieldsSoFar != null && !invalidFieldsSoFar.isEmpty()) {
            return new IndexValidationResults(false, "Mappings do not match. Invalid fields found.", invalidFieldsSoFar);
        } else {
            return IndexValidationResults.success("All fields matched.");
        }
    }

    private static void compareFields(Document expectedFields, Document actualFields, String fieldParent, List<InvalidField> invalidFieldsSoFar) {
        for (String fieldName : expectedFields.keySet()) {
            if (!actualFields.containsKey(fieldName)) {
                invalidFieldsSoFar.add(new InvalidField(fieldParent, fieldName, "Field not found in actual mappings"));
            } else {
                List<Document> expectedFieldsToCheck = getFieldDocuments(expectedFields, fieldName);
                List<Document> actualFieldsToCheck = getFieldDocuments(actualFields, fieldName);

                if (expectedFieldsToCheck.size() != actualFieldsToCheck.size()) {
                    invalidFieldsSoFar.add(new InvalidField(fieldParent, fieldName, "Field count mismatch"));
                } else {
                    for (int i = 0; i < expectedFieldsToCheck.size(); i++) {
                        Document expectedField = expectedFieldsToCheck.get(i);
                        Document actualField = actualFieldsToCheck.get(i);

                        Optional<InvalidField> fieldIsInvalid = compareField(expectedField, actualField, fieldParent, fieldName, invalidFieldsSoFar);
                        fieldIsInvalid.ifPresent(invalidFieldsSoFar::add);
                    }
                }
            }
        }
    }

    private static Optional<InvalidField> compareField(Document expectedField, Document actualField, String fieldParent, String fieldName, List<InvalidField> invalidFieldsSoFar) {
        for (String key : check_equality_of_keys) {
            if (expectedField.containsKey(key)) {
                if (!actualField.containsKey(key)) {
                    return Optional.of(new InvalidField(fieldParent, fieldName, "Expected field not found in actual mappings"));
                }
                Object expectedValue = expectedField.get(key);
                Object actualValue = actualField.get(key);
                if (!expectedValue.equals(actualValue)) {
                    return Optional.of(new InvalidField(fieldParent, fieldName, "Field value mismatch. Expected: '%s', actual: '%s'".formatted(expectedValue, actualValue)));
                }
            }
        }
        if (expectedField.containsKey("fields") && actualField.containsKey("fields")) {
            Document expectedFields = expectedField.get("fields", Document.class);
            Document actualFields = actualField.get("fields", Document.class);
            fieldParent = "%s.%s".formatted(fieldParent, fieldName);
            compareFields(expectedFields, actualFields, fieldParent, invalidFieldsSoFar);
        }
        return Optional.empty();
    }

    public static List<Document> getFieldDocuments(Document fields, String fieldName) {
        Object object = fields.get(fieldName);
        if (object instanceof List<?> objects) {
            return objects.stream()
                    .map(Document.class::cast)
                    .sorted(Comparator.comparing(Document::toJson))
                    .toList();
        }
        return List.of((Document) object);
    }

    public static String readResourceAsString(String resourcePath) {
        try {
            try (InputStream is = IndexValidator.class.getClassLoader().getResourceAsStream(resourcePath)) {
                if (is == null) {
                    throw new IOException("Resource not found: " + resourcePath);
                }
                return new String(is.readAllBytes());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
