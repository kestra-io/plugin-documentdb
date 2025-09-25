package io.kestra.plugin.documentdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.documentdb.models.DocumentDBException;
import io.kestra.plugin.documentdb.models.DocumentDBRecord;
import io.kestra.plugin.documentdb.models.InsertResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HTTP client for interacting with DocumentDB REST API.
 * Handles authentication, request building, and response parsing for MongoDB-compatible operations.
 */
public class DocumentDBClient {

    private static final Logger logger = LoggerFactory.getLogger(DocumentDBClient.class);
    public static final int MAX_DOCUMENTS_PER_INSERT = 10;

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String host;
    private final String username;
    private final String password;

    public DocumentDBClient(String host, String username, String password, RunContext runContext) throws Exception {
        this.host = host;
        this.username = username;
        this.password = password;
        this.objectMapper = new ObjectMapper();
        this.httpClient = HttpClient.builder()
            .runContext(runContext)
            .build();
    }

    /**
     * Insert a single document into a collection.
     */
    public InsertResult insertOne(String database, String collection, Map<String, Object> document) throws Exception {
        String url = buildUrl("insertOne");

        Map<String, Object> requestBody = Map.of(
            "database", database,
            "collection", collection,
            "document", document
        );

        HttpRequest request = HttpRequest.builder()
            .method("POST")
            .uri(URI.create(url))
            .addHeader("Content-Type", "application/json")
            .addHeader("Accept", "application/json")
            .addHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes()))
            .body(HttpRequest.JsonRequestBody.builder()
                .content(requestBody)
                .build())
            .build();

        logger.debug("Making POST request to: {}", url);

        try {
            HttpResponse<String> response = httpClient.request(request, String.class);
            return parseInsertResponse(response.getBody());
        } catch (HttpClientResponseException e) {
            String statusCode = e.getResponse() != null ? String.valueOf(e.getResponse().getStatus().getCode()) : "unknown";
            String responseBody = e.getResponse() != null ? String.valueOf(e.getResponse().getBody()) : "unknown";
            throw new DocumentDBException("Failed to insert document: " + statusCode + " - " + responseBody);
        }
    }

    /**
     * Insert multiple documents into a collection.
     */
    public InsertResult insertMany(String database, String collection, List<Map<String, Object>> documents) throws Exception {
        if (documents.size() > MAX_DOCUMENTS_PER_INSERT) {
            throw new IllegalArgumentException("Cannot insert more than " + MAX_DOCUMENTS_PER_INSERT + " documents at once");
        }

        String url = buildUrl("insertMany");

        Map<String, Object> requestBody = Map.of(
            "database", database,
            "collection", collection,
            "documents", documents
        );

        HttpRequest request = HttpRequest.builder()
            .method("POST")
            .uri(URI.create(url))
            .addHeader("Content-Type", "application/json")
            .addHeader("Accept", "application/json")
            .addHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes()))
            .body(HttpRequest.JsonRequestBody.builder()
                .content(requestBody)
                .build())
            .build();

        logger.debug("Making POST request to: {}", url);

        try {
            HttpResponse<String> response = httpClient.request(request, String.class);
            return parseInsertResponse(response.getBody());
        } catch (HttpClientResponseException e) {
            String statusCode = e.getResponse() != null ? String.valueOf(e.getResponse().getStatus().getCode()) : "unknown";
            String responseBody = e.getResponse() != null ? String.valueOf(e.getResponse().getBody()) : "unknown";
            throw new DocumentDBException("Failed to insert documents: " + statusCode + " - " + responseBody);
        }
    }

    /**
     * Find documents in a collection with optional filter and limit.
     */
    public List<DocumentDBRecord> find(String database, String collection, Map<String, Object> filter,
                                     Integer limit, Integer skip) throws Exception {
        String url = buildUrl("find");

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("database", database);
        requestBody.put("collection", collection);

        if (filter != null && !filter.isEmpty()) {
            requestBody.put("filter", filter);
        }
        if (limit != null) {
            requestBody.put("limit", limit);
        }
        if (skip != null) {
            requestBody.put("skip", skip);
        }

        HttpRequest request = HttpRequest.builder()
            .method("POST")
            .uri(URI.create(url))
            .addHeader("Content-Type", "application/json")
            .addHeader("Accept", "application/json")
            .addHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes()))
            .body(HttpRequest.JsonRequestBody.builder()
                .content(requestBody)
                .build())
            .build();

        logger.debug("Making POST request to: {}", url);

        try {
            HttpResponse<String> response = httpClient.request(request, String.class);
            return parseFindResponse(response.getBody());
        } catch (HttpClientResponseException e) {
            String statusCode = e.getResponse() != null ? String.valueOf(e.getResponse().getStatus().getCode()) : "unknown";
            String responseBody = e.getResponse() != null ? String.valueOf(e.getResponse().getBody()) : "unknown";
            throw new DocumentDBException("Failed to find documents: " + statusCode + " - " + responseBody);
        }
    }

    /**
     * Execute an aggregation pipeline on a collection.
     */
    public List<DocumentDBRecord> aggregate(String database, String collection, List<Map<String, Object>> pipeline) throws Exception {
        String url = buildUrl("aggregate");

        Map<String, Object> requestBody = Map.of(
            "database", database,
            "collection", collection,
            "pipeline", pipeline
        );

        HttpRequest request = HttpRequest.builder()
            .method("POST")
            .uri(URI.create(url))
            .addHeader("Content-Type", "application/json")
            .addHeader("Accept", "application/json")
            .addHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes()))
            .body(HttpRequest.JsonRequestBody.builder()
                .content(requestBody)
                .build())
            .build();

        logger.debug("Making POST request to: {}", url);

        try {
            HttpResponse<String> response = httpClient.request(request, String.class);
            return parseFindResponse(response.getBody());
        } catch (HttpClientResponseException e) {
            String statusCode = e.getResponse() != null ? String.valueOf(e.getResponse().getStatus().getCode()) : "unknown";
            String responseBody = e.getResponse() != null ? String.valueOf(e.getResponse().getBody()) : "unknown";
            throw new DocumentDBException("Failed to execute aggregation: " + statusCode + " - " + responseBody);
        }
    }

    /**
     * Build URL for DocumentDB HTTP API endpoint.
     */
    private String buildUrl(String action) {
        // Remove trailing slash if present
        String baseUrl = host.endsWith("/") ?
            host.substring(0, host.length() - 1) : host;
        return baseUrl + "/data/v1/action/" + action;
    }

    /**
     * Parse insert response from DocumentDB API.
     */
    private InsertResult parseInsertResponse(String responseBody) throws JsonProcessingException {
        JsonNode jsonNode = objectMapper.readTree(responseBody);

        List<String> insertedIds = new ArrayList<>();
        Integer insertedCount = 0;

        if (jsonNode.has("insertedId")) {
            // Single insert
            insertedIds.add(jsonNode.get("insertedId").asText());
            insertedCount = 1;
        } else if (jsonNode.has("insertedIds")) {
            // Multiple inserts
            JsonNode idsNode = jsonNode.get("insertedIds");
            if (idsNode.isArray()) {
                for (JsonNode idNode : idsNode) {
                    insertedIds.add(idNode.asText());
                }
            }
            insertedCount = insertedIds.size();
        }

        return new InsertResult(insertedIds, insertedCount);
    }

    /**
     * Parse find/aggregate response from DocumentDB API.
     */
    private List<DocumentDBRecord> parseFindResponse(String responseBody) throws JsonProcessingException {
        JsonNode jsonNode = objectMapper.readTree(responseBody);

        List<DocumentDBRecord> records = new ArrayList<>();
        JsonNode documentsNode = jsonNode.get("documents");

        if (documentsNode != null && documentsNode.isArray()) {
            for (JsonNode documentNode : documentsNode) {
                String id = null;
                if (documentNode.has("_id")) {
                    JsonNode idNode = documentNode.get("_id");
                    if (idNode.has("$oid")) {
                        id = idNode.get("$oid").asText();
                    } else {
                        id = idNode.asText();
                    }
                }

                Map<String, Object> fields = objectMapper.convertValue(documentNode, Map.class);
                records.add(new DocumentDBRecord(id, fields));
            }
        }

        return records;
    }
}