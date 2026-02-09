package dev.onemount.iceburg.service;

import dev.onemount.iceburg.repository.OpenMetadataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class OpenMetadataService {

    private final OpenMetadataRepository openMetadataRepository;

    public List<Map<String, String>> listDatabaseServices(String authHeader) {
        HttpHeaders headers = createHeadersFromAuth(authHeader);
        Map<String, Object> responseBody = openMetadataRepository.listDatabaseServices(headers);

        List<Map<String, Object>> services = (List<Map<String, Object>>) responseBody.get("data");
        List<Map<String, String>> simplifiedServices = new ArrayList<>();

        for (Map<String, Object> service : services) {
            Map<String, String> serviceInfo = new LinkedHashMap<>();
            serviceInfo.put("name", (String) service.get("name"));
            serviceInfo.put("serviceType", (String) service.get("serviceType"));
            serviceInfo.put("id", (String) service.get("id"));
            simplifiedServices.add(serviceInfo);
        }

        return simplifiedServices;
    }

    public void pushLineageEdge(String authHeader, String fromTableFqn, String toTableFqn) {
        HttpHeaders headers = createHeadersFromAuth(authHeader);

        String fromUuid = openMetadataRepository.getTableUuidByFqn(headers, fromTableFqn);
        String toUuid = openMetadataRepository.getTableUuidByFqn(headers, toTableFqn);

        openMetadataRepository.addLineageEdge(headers, fromUuid, toUuid);

        log.info("Lineage edge created: {} ({}) -> {} ({})", fromTableFqn, fromUuid, toTableFqn, toUuid);
    }

    private HttpHeaders createHeadersFromAuth(String authHeader) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        String token = null;

        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            token = authHeader.substring(7);
        } else if (authHeader != null && !authHeader.isEmpty()) {
            token = authHeader;
        }

        if (token != null && !token.isEmpty()) {
            headers.set("Authorization", "Bearer " + token);
        }

        return headers;
    }

    public String triggerIngestion(String authHeader, String serviceName) {
        HttpHeaders headers = createHeadersFromAuth(authHeader);

        List<Map<String, Object>> pipelines = openMetadataRepository.listIngestionPipelines(headers, serviceName);

        if (pipelines.isEmpty()) {
            throw new RuntimeException("No ingestion pipeline found for service: " + serviceName);
        }

        Map<String, Object> metadataPipeline = pipelines.stream()
                .filter(p -> "metadata".equals(p.get("pipelineType")))
                .findFirst()
                .orElse(pipelines.get(0));

        String pipelineId = (String) metadataPipeline.get("id");
        String pipelineName = (String) metadataPipeline.get("name");

        log.info("Triggering ingestion pipeline: {} ({})", pipelineName, pipelineId);

        openMetadataRepository.triggerIngestionPipeline(headers, pipelineId);

        return pipelineId;
    }

    public boolean waitForTableDiscovery(String authHeader, String fqn, int maxRetries, long delayMs) {
        HttpHeaders headers = createHeadersFromAuth(authHeader);
        return openMetadataRepository.waitForTableDiscovery(headers, fqn, maxRetries, delayMs);
    }

    public Map<String, Object> getIngestionStatus(String authHeader, String pipelineId) {
        HttpHeaders headers = createHeadersFromAuth(authHeader);
        return openMetadataRepository.getIngestionPipelineStatus(headers, pipelineId);
    }
}
