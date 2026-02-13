package dev.onemount.iceburg.controller;

import dev.onemount.iceburg.dto.response.LineageDataResponse;
import dev.onemount.iceburg.dto.response.LineageFlowResponse;
import dev.onemount.iceburg.dto.response.MarquezLineageResponse;
import dev.onemount.iceburg.repository.LineageTableRepository;
import dev.onemount.iceburg.service.LineageService;
import dev.onemount.iceburg.service.OpenMetadataService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@Slf4j
@RestController
@RequestMapping("/api/lineage")
@RequiredArgsConstructor
public class LineageController {

    private final LineageService lineageService;
    private final OpenMetadataService openMetadataService;
    private final LineageTableRepository lineageTableRepository;

    @PostMapping("/demo/create-flow")
    public ResponseEntity<LineageFlowResponse> createDemoLineageFlow(
            @RequestHeader(value = "Authorization", required = false) String authHeader,
            @RequestParam(defaultValue = "iceberg") String serviceName) {
        try {
            LineageFlowResponse response = lineageService.createDemoLineageFlow(authHeader, serviceName);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error creating lineage flow", e);

            LineageFlowResponse errorResponse = LineageFlowResponse.builder()
                    .success(false)
                    .message("Failed to create lineage: " + e.getMessage())
                    .build();

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(errorResponse);
        }
    }

    @GetMapping("/demo/show-data")
    public ResponseEntity<LineageDataResponse> showLineageData() {
        try {
            LineageDataResponse response = lineageService.getLineageData();
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error retrieving lineage data", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping("/services")
    public ResponseEntity<Map<String, Object>> listDatabaseServices(
            @RequestHeader(value = "Authorization", required = false) String authHeader) {
        Map<String, Object> result = new LinkedHashMap<>();

        try {
            List<Map<String, String>> services = openMetadataService.listDatabaseServices(authHeader);

            result.put("success", true);
            result.put("services", services);
            result.put("count", services.size());

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("Error listing services", e);
            result.put("success", false);
            result.put("error", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }

    @PostMapping("/push-lineage")
    public ResponseEntity<Map<String, Object>> pushLineageToOpenMetadata(
            @RequestHeader(value = "Authorization", required = false) String authHeader,
            @RequestParam(defaultValue = "iceberg") String serviceName) {
        try {
            Map<String, Object> result = lineageService.pushLineageEdges(authHeader, serviceName);
            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("Error pushing lineage", e);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("success", false);
            result.put("error", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }

    @PostMapping("/trigger-ingestion")
    public ResponseEntity<Map<String, Object>> triggerIngestion(
            @RequestHeader(value = "Authorization", required = false) String authHeader,
            @RequestParam(defaultValue = "iceberg") String serviceName) {
        Map<String, Object> result = new LinkedHashMap<>();

        try {
            String pipelineId = openMetadataService.triggerIngestion(authHeader, serviceName);

            result.put("success", true);
            result.put("message", "Ingestion pipeline triggered successfully");
            result.put("pipelineId", pipelineId);
            result.put("serviceName", serviceName);

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("Error triggering ingestion", e);
            result.put("success", false);
            result.put("error", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }

    @GetMapping("/ingestion-status/{pipelineId}")
    public ResponseEntity<Map<String, Object>> getIngestionStatus(
            @RequestHeader(value = "Authorization", required = false) String authHeader,
            @PathVariable String pipelineId) {
        try {
            Map<String, Object> status = openMetadataService.getIngestionStatus(authHeader, pipelineId);
            return ResponseEntity.ok(status);

        } catch (Exception e) {
            log.error("Error getting ingestion status", e);
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("success", false);
            error.put("error", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    @PostMapping("/auto-lineage")
    public ResponseEntity<MarquezLineageResponse> runAutoLineage() {
        try {
            lineageTableRepository.createNamespaces();
            lineageTableRepository.createStagingTable();
            lineageTableRepository.transformToProduction();
            lineageTableRepository.createAnalytics();

            MarquezLineageResponse response = MarquezLineageResponse.builder()
                    .success(true)
                    .message("Spark operations completed. Lineage captured automatically by OpenLineage.")
                    .marquezUrl("http://localhost:3000")
                    .jobName("iceberg_lineage_job")
                    .capturedEvents(List.of(
                            "Table Creation Events",
                            "Data Transformation Events",
                            "Write Operations"
                    ))
                    .build();

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error running auto lineage", e);
            MarquezLineageResponse errorResponse = MarquezLineageResponse.builder()
                    .success(false)
                    .message("Failed: " + e.getMessage())
                    .build();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}
