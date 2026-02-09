package dev.onemount.iceburg.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Repository;
import org.springframework.web.client.RestTemplate;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
@RequiredArgsConstructor
public class OpenMetadataRepository {

    private final RestTemplate restTemplate;

    @Value("${openmetadata.api.url:http://localhost:8585/api}")
    private String openMetadataUrl;

    public Map<String, Object> listDatabaseServices(HttpHeaders headers) {
        HttpEntity<String> entity = new HttpEntity<>(headers);

        ResponseEntity<Map> response = restTemplate.exchange(
                openMetadataUrl + "/v1/services/databaseServices?limit=100",
                HttpMethod.GET,
                entity,
                Map.class
        );

        return response.getBody();
    }

    public String getTableUuidByFqn(HttpHeaders headers, String fqn) {
        String encodedFqn = URLEncoder.encode(fqn, StandardCharsets.UTF_8);
        String url = openMetadataUrl + "/v1/tables/name/" + encodedFqn;

        log.info("Fetching UUID for FQN: {} from URL: {}", fqn, url);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        ResponseEntity<Map> response = restTemplate.exchange(
                java.net.URI.create(url),
                HttpMethod.GET,
                entity,
                Map.class
        );

        Map<String, Object> tableData = response.getBody();
        String uuid = (String) tableData.get("id");

        if (uuid == null || uuid.isEmpty()) {
            throw new RuntimeException("UUID not found in response for FQN: " + fqn);
        }

        log.info("Resolved FQN {} to UUID {}", fqn, uuid);
        return uuid;
    }

    public void addLineageEdge(HttpHeaders headers, String fromUuid, String toUuid) {
        String lineagePayload = String.format("""
            {
              "edge": {
                "fromEntity": {
                  "id": "%s",
                  "type": "table"
                },
                "toEntity": {
                  "id": "%s",
                  "type": "table"
                }
              }
            }
            """, fromUuid, toUuid);

        HttpEntity<String> entity = new HttpEntity<>(lineagePayload, headers);

        restTemplate.exchange(
                openMetadataUrl + "/v1/lineage",
                HttpMethod.PUT,
                entity,
                String.class
        );

        log.info("Added lineage edge: {} -> {}", fromUuid, toUuid);
    }

    public String triggerIngestionPipeline(HttpHeaders headers, String pipelineId) {
        String url = openMetadataUrl + "/v1/services/ingestionPipelines/trigger/" + pipelineId;

        log.info("Triggering ingestion pipeline: {}", pipelineId);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        ResponseEntity<Map> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                entity,
                Map.class
        );

        return (String) response.getBody().get("id");
    }

    public Map<String, Object> getIngestionPipelineStatus(HttpHeaders headers, String pipelineId) {
        String url = openMetadataUrl + "/v1/services/ingestionPipelines/" + pipelineId;

        HttpEntity<String> entity = new HttpEntity<>(headers);

        ResponseEntity<Map> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                Map.class
        );

        return response.getBody();
    }

    public List<Map<String, Object>> listIngestionPipelines(HttpHeaders headers, String serviceName) {
        String url = openMetadataUrl + "/v1/services/ingestionPipelines?service=" + serviceName + "&limit=100";

        log.info("Listing ingestion pipelines for service: {}", serviceName);

        HttpEntity<String> entity = new HttpEntity<>(headers);

        ResponseEntity<Map> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                entity,
                Map.class
        );

        Map<String, Object> body = response.getBody();
        List<Map<String, Object>> pipelines = (List<Map<String, Object>>) body.get("data");

        log.info("Found {} ingestion pipelines", pipelines != null ? pipelines.size() : 0);

        return pipelines != null ? pipelines : new ArrayList<>();
    }

    public boolean waitForTableDiscovery(HttpHeaders headers, String fqn, int maxRetries, long delayMs) {
        log.info("Waiting for table {} to be discovered (max {} retries, {}ms delay)", fqn, maxRetries, delayMs);

        for (int i = 0; i < maxRetries; i++) {
            try {
                getTableUuidByFqn(headers, fqn);
                log.info("Table {} discovered successfully after {} attempts", fqn, i + 1);
                return true;
            } catch (Exception e) {
                if (i < maxRetries - 1) {
                    log.debug("Attempt {}/{}: Table {} not found yet, waiting...", i + 1, maxRetries, fqn);
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("Wait interrupted", ie);
                        return false;
                    }
                } else {
                    log.warn("Table {} not found after {} attempts", fqn, maxRetries);
                }
            }
        }
        return false;
    }
}
