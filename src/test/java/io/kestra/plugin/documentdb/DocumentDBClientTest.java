package io.kestra.plugin.documentdb;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CountOptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DocumentDBClientTest {

    private static final String CERTIFICATE_BUNDLE_RESOURCE = "/io/kestra/plugin/documentdb/multi-certificates.pem";

    @Test
    void shouldLoadEveryCertificateInPemBundle() throws Exception {
        String certificateBundle = readCertificateBundle();

        assertNotNull(DocumentDBClient.createSslContext(certificateBundle));

        Collection<? extends Certificate> certificates;
        try (
            ByteArrayInputStream inputStream = new ByteArrayInputStream(
                certificateBundle.getBytes(StandardCharsets.US_ASCII)
            )
        ) {
            certificates = CertificateFactory.getInstance("X.509").generateCertificates(inputStream);
        }

        assertEquals(2, certificates.size());

        KeyStore trustStore = DocumentDBClient.createTrustStore(certificates);
        Set<String> aliases = new HashSet<>();
        Set<Certificate> trustedCertificates = new HashSet<>();
        for (String alias : Collections.list(trustStore.aliases())) {
            aliases.add(alias);
            trustedCertificates.add(trustStore.getCertificate(alias));
        }

        assertEquals(Set.of("documentdb-ca-0", "documentdb-ca-1"), aliases);
        assertEquals(new HashSet<>(certificates), trustedCertificates);
    }

    @Test
    void shouldRejectPemWithoutCertificates() {
        assertThrows(
            CertificateException.class,
            () -> DocumentDBClient.createSslContext("not a certificate")
        );
    }

    @Test
    void shouldFailClearlyForEmptyCertificateCollection() {
        GeneralSecurityException exception = assertThrows(
            GeneralSecurityException.class,
            () -> DocumentDBClient.createTrustStore(List.of())
        );

        assertTrue(exception.getMessage().contains("contains no certificates"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldPreserveOutPipelineWithoutAppendingLimit() {
        MongoClient mongoClient = mock(MongoClient.class);
        MongoDatabase database = mock(MongoDatabase.class);
        MongoCollection<Document> collection = mock(MongoCollection.class);
        AggregateIterable<Document> iterable = mock(AggregateIterable.class);
        List<Map<String, Object>> pipeline = List.of(
            Map.of("$match", Map.of("active", true)),
            Map.of("$out", "archived_records")
        );
        List<Document> expectedPipeline = List.of(
            new Document("$match", new Document("active", true)),
            new Document("$out", "archived_records")
        );
        when(mongoClient.getDatabase("test_db")).thenReturn(database);
        when(database.getCollection("records")).thenReturn(collection);
        when(collection.aggregate(expectedPipeline)).thenReturn(iterable);
        DocumentDBClient client = new DocumentDBClient(mongoClient);

        client.aggregateIterable("test_db", "records", pipeline);

        verify(collection).aggregate(expectedPipeline);
    }

    @Test
    void shouldCountDocumentsWithTheRequestedFilterSkipAndLimit() throws Exception {
        MongoClient mongoClient = mock(MongoClient.class);
        MongoDatabase database = mock(MongoDatabase.class);
        MongoCollection<Document> collection = mock(MongoCollection.class);
        when(mongoClient.getDatabase("test_db")).thenReturn(database);
        when(database.getCollection("records")).thenReturn(collection);
        when(collection.countDocuments(any(Bson.class), any(CountOptions.class))).thenReturn(2L);
        DocumentDBClient client = new DocumentDBClient(mongoClient);

        assertEquals(
            2L, client.countDocuments(
                "test_db",
                "records",
                Map.of("active", true),
                5,
                2
            )
        );

        ArgumentCaptor<Bson> filter = ArgumentCaptor.forClass(Bson.class);
        ArgumentCaptor<CountOptions> options = ArgumentCaptor.forClass(CountOptions.class);
        verify(collection).countDocuments(filter.capture(), options.capture());
        assertEquals(new Document("active", true), filter.getValue());
        assertEquals(5, options.getValue().getLimit());
        assertEquals(2L, options.getValue().getSkip());
    }

    private String readCertificateBundle() throws IOException {
        try (
            var inputStream = DocumentDBClientTest.class.getResourceAsStream(
                CERTIFICATE_BUNDLE_RESOURCE
            )
        ) {
            assertNotNull(inputStream, "Certificate bundle fixture must be available");
            return new String(inputStream.readAllBytes(), StandardCharsets.US_ASCII);
        }
    }
}
