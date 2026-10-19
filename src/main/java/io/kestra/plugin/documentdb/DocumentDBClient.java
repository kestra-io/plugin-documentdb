package io.kestra.plugin.documentdb;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;

import io.kestra.plugin.documentdb.models.DeleteResult;
import io.kestra.plugin.documentdb.models.DocumentDBException;
import io.kestra.plugin.documentdb.models.DocumentDBRecord;
import io.kestra.plugin.documentdb.models.InsertResult;
import io.kestra.plugin.documentdb.models.UpdateResult;

/**
 * MongoDB driver-backed client for DocumentDB operations.
 */
public class DocumentDBClient implements AutoCloseable {

    private final MongoClient mongoClient;

    public DocumentDBClient(String connectionString, String caCertificate) throws GeneralSecurityException, IOException {
        MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(connectionString));

        if (caCertificate != null && !caCertificate.isBlank()) {
            SSLContext sslContext = createSslContext(caCertificate);
            settingsBuilder.applyToSslSettings(sslSettings -> sslSettings
                .enabled(true)
                .context(sslContext));
        }

        this.mongoClient = MongoClients.create(settingsBuilder.build());
    }

    DocumentDBClient(MongoClient mongoClient) {
        this.mongoClient = Objects.requireNonNull(mongoClient);
    }

    static SSLContext createSslContext(String caCertificate)
        throws GeneralSecurityException, IOException {

        CertificateFactory certificateFactory =
            CertificateFactory.getInstance("X.509");

        Collection<? extends Certificate> certificates;

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(
            caCertificate.getBytes(StandardCharsets.UTF_8)
        )) {
            certificates = certificateFactory.generateCertificates(inputStream);
        }

        KeyStore trustStore = createTrustStore(certificates);

        TrustManagerFactory trustManagerFactory =
            TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm()
            );

        trustManagerFactory.init(trustStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), null);

        return sslContext;
    }

    static KeyStore createTrustStore(
        Collection<? extends Certificate> certificates
    ) throws GeneralSecurityException, IOException {

        if (certificates.isEmpty()) {
            throw new GeneralSecurityException(
                "The supplied CA certificate bundle contains no certificates"
            );
        }

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);

        int index = 0;

        for (Certificate certificate : certificates) {
            trustStore.setCertificateEntry(
                "documentdb-ca-" + index,
                certificate
            );
            index++;
        }

        return trustStore;
    }

    @Override
    public void close() {
        mongoClient.close();
    }

    public InsertResult insertOne(String database, String collection, Map<String, Object> document) throws Exception {
        try {
            MongoCollection<Document> mongoCollection = mongoClient.getDatabase(database).getCollection(collection);
            Document documentToInsert = toDocument(document);
            InsertOneResult result = mongoCollection.insertOne(documentToInsert);
            return new InsertResult(List.of(toIdString(result.getInsertedId())), 1);
        } catch (Exception e) {
            throw new DocumentDBException("Failed to insert document: " + e.getMessage(), e);
        }
    }

    public InsertResult insertMany(String database, String collection, List<Map<String, Object>> documents) throws Exception {
        try {
            MongoCollection<Document> mongoCollection = mongoClient.getDatabase(database).getCollection(collection);
            List<Document> documentsToInsert = documents.stream().map(this::toDocument).toList();
            InsertManyResult result = mongoCollection.insertMany(documentsToInsert);
            List<String> insertedIds = result.getInsertedIds().values().stream()
                .map(this::toIdString)
                .toList();
            return new InsertResult(insertedIds, insertedIds.size());
        } catch (Exception e) {
            throw new DocumentDBException("Failed to insert documents: " + e.getMessage(), e);
        }
    }

    public List<DocumentDBRecord> find(String database, String collection, Map<String, Object> filter,
        Integer limit, Integer skip) throws Exception {
        try {
            List<DocumentDBRecord> records = new ArrayList<>();
            try (MongoCursor<Document> cursor = findIterable(database, collection, filter, limit, skip).iterator()) {
                while (cursor.hasNext()) {
                    records.add(toRecord(cursor.next()));
                }
            }
            return records;
        } catch (Exception e) {
            throw new DocumentDBException("Failed to find documents: " + e.getMessage(), e);
        }
    }

    FindIterable<Document> findIterable(String database, String collection, Map<String, Object> filter,
        Integer limit, Integer skip) {
        MongoCollection<Document> mongoCollection = mongoClient.getDatabase(database).getCollection(collection);
        Document filterDocument = filter == null || filter.isEmpty() ? new Document() : toDocument(filter);
        FindIterable<Document> iterable = mongoCollection.find(filterDocument);
        if (skip != null) {
            iterable = iterable.skip(skip);
        }
        if (limit != null) {
            iterable = iterable.limit(limit);
        }
        return iterable;
    }

    public List<DocumentDBRecord> aggregate(String database, String collection, List<Map<String, Object>> pipeline) throws Exception {
        try {
            List<DocumentDBRecord> records = new ArrayList<>();
            try (MongoCursor<Document> cursor = aggregateIterable(database, collection, pipeline).iterator()) {
                while (cursor.hasNext()) {
                    records.add(toRecord(cursor.next()));
                }
            }
            return records;
        } catch (Exception e) {
            throw new DocumentDBException("Failed to execute aggregation: " + e.getMessage(), e);
        }
    }

    AggregateIterable<Document> aggregateIterable(
        String database,
        String collection,
        List<Map<String, Object>> pipeline
    ) {
        MongoCollection<Document> mongoCollection = mongoClient.getDatabase(database).getCollection(collection);
        List<Document> pipelineDocuments = pipeline.stream().map(this::toDocument).toList();
        return mongoCollection.aggregate(pipelineDocuments);
    }

    public UpdateResult updateOne(String database, String collection, Map<String, Object> filter, Map<String, Object> update) throws Exception {
        try {
            MongoCollection<Document> mongoCollection = mongoClient.getDatabase(database).getCollection(collection);
            Document filterDocument = filter == null || filter.isEmpty() ? new Document() : toDocument(filter);
            Document updateDocument = toDocument(update);
            com.mongodb.client.result.UpdateResult result = mongoCollection.updateOne(filterDocument, updateDocument);
            return new UpdateResult((int) result.getMatchedCount(), (int) result.getModifiedCount(), toIdString(result.getUpsertedId()));
        } catch (Exception e) {
            throw new DocumentDBException("Failed to update document: " + e.getMessage(), e);
        }
    }

    public UpdateResult updateMany(String database, String collection, Map<String, Object> filter, Map<String, Object> update) throws Exception {
        try {
            MongoCollection<Document> mongoCollection = mongoClient.getDatabase(database).getCollection(collection);
            Document filterDocument = filter == null || filter.isEmpty() ? new Document() : toDocument(filter);
            Document updateDocument = toDocument(update);
            com.mongodb.client.result.UpdateResult result = mongoCollection.updateMany(filterDocument, updateDocument);
            return new UpdateResult((int) result.getMatchedCount(), (int) result.getModifiedCount(), toIdString(result.getUpsertedId()));
        } catch (Exception e) {
            throw new DocumentDBException("Failed to update documents: " + e.getMessage(), e);
        }
    }

    public DeleteResult deleteOne(String database, String collection, Map<String, Object> filter) throws Exception {
        try {
            MongoCollection<Document> mongoCollection = mongoClient.getDatabase(database).getCollection(collection);
            Document filterDocument = filter == null || filter.isEmpty() ? new Document() : toDocument(filter);
            com.mongodb.client.result.DeleteResult result = mongoCollection.deleteOne(filterDocument);
            return new DeleteResult((int) result.getDeletedCount());
        } catch (Exception e) {
            throw new DocumentDBException("Failed to delete document: " + e.getMessage(), e);
        }
    }

    public DeleteResult deleteMany(String database, String collection, Map<String, Object> filter) throws Exception {
        try {
            MongoCollection<Document> mongoCollection = mongoClient.getDatabase(database).getCollection(collection);
            Document filterDocument = filter == null || filter.isEmpty() ? new Document() : toDocument(filter);
            com.mongodb.client.result.DeleteResult result = mongoCollection.deleteMany(filterDocument);
            return new DeleteResult((int) result.getDeletedCount());
        } catch (Exception e) {
            throw new DocumentDBException("Failed to delete documents: " + e.getMessage(), e);
        }
    }

    private Document toDocument(Map<String, Object> source) {
        Document document = new Document();
        if (source == null) {
            return document;
        }
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            document.put(entry.getKey(), convertValue(entry.getValue()));
        }
        return document;
    }

    private Object convertValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Document nested = new Document();
            map.forEach((key, nestedValue) -> nested.put(String.valueOf(key), convertValue(nestedValue)));
            return nested;
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> converted = new ArrayList<>();
            for (Object item : iterable) {
                converted.add(convertValue(item));
            }
            return converted;
        }
        return value;
    }

    DocumentDBRecord toRecord(Document document) {
        Map<String, Object> fields = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            if (!"_id".equals(entry.getKey())) {
                fields.put(entry.getKey(), toJavaValue(entry.getValue()));
            }
        }
        return new DocumentDBRecord(toIdString(document.get("_id")), fields);
    }

    private Object toJavaValue(Object value) {
        if (value instanceof Document document) {
            Map<String, Object> converted = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : document.entrySet()) {
                converted.put(entry.getKey(), toJavaValue(entry.getValue()));
            }
            return converted;
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> converted = new ArrayList<>();
            for (Object item : iterable) {
                converted.add(toJavaValue(item));
            }
            return converted;
        }
        if (value instanceof ObjectId objectId) {
            return objectId.toHexString();
        }
        if (value instanceof BsonString bsonString) {
            return bsonString.getValue();
        }
        if (value instanceof BsonValue bsonValue) {
            if (bsonValue.isObjectId()) {
                return bsonValue.asObjectId().getValue().toHexString();
            }
            return bsonValue.toString();
        }
        return value;
    }

    private String toIdString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof ObjectId objectId) {
            return objectId.toHexString();
        }
        if (value instanceof BsonObjectId bsonObjectId) {
            return bsonObjectId.getValue().toHexString();
        }
        if (value instanceof BsonString bsonString) {
            return bsonString.getValue();
        }
        if (value instanceof BsonValue bsonValue) {
            if (bsonValue.isObjectId()) {
                return bsonValue.asObjectId().getValue().toHexString();
            }
            return bsonValue.toString();
        }
        return value.toString();
    }
}
