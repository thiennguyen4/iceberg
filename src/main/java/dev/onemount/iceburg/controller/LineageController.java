package dev.onemount.iceburg.controller;

import dev.onemount.iceburg.dto.response.LineageDataResponse;
import dev.onemount.iceburg.dto.response.LineageFlowResponse;
import dev.onemount.iceburg.dto.response.TableDataResponse;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

@Slf4j
@RestController
@RequestMapping("/api/lineage")
@RequiredArgsConstructor
public class LineageController {

    private final SparkSession spark;
    private final RestTemplate restTemplate;

    @Value("${openmetadata.api.url:http://localhost:8585/api}")
    private String openMetadataUrl;

    @PostMapping("/demo/create-flow")
    public ResponseEntity<LineageFlowResponse> createDemoLineageFlow() {
        try {
            createNamespaces();
            createStagingTable();
            transformToProduction();
            createAnalytics();

            LineageFlowResponse response = LineageFlowResponse.builder()
                    .success(true)
                    .message("Tables created successfully! Steps: 1) POST /api/lineage/trigger-ingestion/{serviceName} to sync tables, 2) POST /api/lineage/push-lineage?serviceName={name} to create lineage relationships")
                    .flowDetails(LineageFlowResponse.LineageFlowDetails.builder()
                            .stagingTable("demo.staging.orders")
                            .productionTable("demo.production.orders_clean")
                            .analyticsTable("demo.analytics.daily_summary")
                            .lineageStatus("Tables created. Next: Trigger ingestion, then push lineage metadata")
                            .build())
                    .build();

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
            TableDataResponse staging = convertToTableData(
                    "iceburg.default.staging.\"staging.orders\"",
                    spark.sql("SELECT * FROM demo.staging.orders")
            );

            TableDataResponse production = convertToTableData(
                    "iceburg.default.production.\"production.orders_clean\"",
                    spark.sql("SELECT * FROM demo.production.orders_clean")
            );

            TableDataResponse analytics = convertToTableData(
                    "iceburg.default.analytics.\"analytics.daily_summary\"",
                    spark.sql("SELECT * FROM demo.analytics.daily_summary")
            );

            LineageDataResponse response = LineageDataResponse.builder()
                    .staging(staging)
                    .production(production)
                    .analytics(analytics)
                    .lineageFlow("staging.orders → production.orders_clean → analytics.daily_summary")
                    .build();

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error retrieving lineage data", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/trigger-ingestion/{serviceName}")
    public ResponseEntity<Map<String, Object>> triggerIngestionForService(
            @PathVariable String serviceName,
            @RequestHeader(value = "Authorization", required = false) String authHeader) {
        Map<String, Object> result = new LinkedHashMap<>();

        try {
            log.info("Triggering metadata ingestion for service: {}", serviceName);

            String pipelineName = findIngestionPipeline(serviceName, authHeader);

            if (pipelineName == null) {
                result.put("success", false);
                result.put("error", "No ingestion pipeline found for service: " + serviceName);
                result.put("hint", "Please create an ingestion pipeline in OpenMetadata UI: " +
                    "Settings → Services → " + serviceName + " → Ingestions → Add Ingestion");
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
            }

            triggerIngestion(pipelineName, authHeader);

            result.put("success", true);
            result.put("serviceName", serviceName);
            result.put("pipelineName", pipelineName);
            result.put("message", "Ingestion triggered successfully! Tables will appear in OpenMetadata in ~30-60 seconds.");
            result.put("nextSteps", List.of(
                "1. Wait 30-60 seconds for ingestion to complete",
                "2. Go to http://localhost:8585/explore/tables",
                "3. Search for: demo.staging.orders, demo.production.orders_clean, demo.analytics.daily_summary",
                "4. Click any table → Lineage tab to see the flow"
            ));

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("Error triggering ingestion", e);
            result.put("success", false);
            result.put("error", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }

    @GetMapping("/services")
    public ResponseEntity<Map<String, Object>> listDatabaseServices(
            @RequestHeader(value = "Authorization", required = false) String authHeader) {
        Map<String, Object> result = new LinkedHashMap<>();

        try {
            HttpHeaders headers = createHeadersFromAuth(authHeader);
            HttpEntity<String> entity = new HttpEntity<>(headers);

            ResponseEntity<Map> response = restTemplate.exchange(
                openMetadataUrl + "/v1/services/databaseServices?limit=100",
                HttpMethod.GET,
                entity,
                Map.class
            );

            Map<String, Object> responseBody = response.getBody();
            List<Map<String, Object>> services = (List<Map<String, Object>>) responseBody.get("data");

            List<Map<String, String>> simplifiedServices = new ArrayList<>();
            for (Map<String, Object> service : services) {
                Map<String, String> serviceInfo = new LinkedHashMap<>();
                serviceInfo.put("name", (String) service.get("name"));
                serviceInfo.put("serviceType", (String) service.get("serviceType"));
                serviceInfo.put("id", (String) service.get("id"));
                simplifiedServices.add(serviceInfo);
            }

            result.put("success", true);
            result.put("services", simplifiedServices);
            result.put("count", simplifiedServices.size());
            result.put("hint", "To trigger ingestion, call: POST /api/lineage/trigger-ingestion/{serviceName}");

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("Error listing services", e);
            result.put("success", false);
            result.put("error", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }

    @GetMapping("/services/{serviceName}/pipelines")
    public ResponseEntity<Map<String, Object>> getServicePipelines(
            @PathVariable String serviceName,
            @RequestHeader(value = "Authorization", required = false) String authHeader) {
        Map<String, Object> result = new LinkedHashMap<>();

        try {
            HttpHeaders headers = createHeadersFromAuth(authHeader);
            HttpEntity<String> entity = new HttpEntity<>(headers);

            ResponseEntity<Map> response = restTemplate.exchange(
                openMetadataUrl + "/v1/services/ingestionPipelines?service=" + serviceName + "&limit=100",
                HttpMethod.GET,
                entity,
                Map.class
            );

            Map<String, Object> responseBody = response.getBody();
            List<Map<String, Object>> pipelines = (List<Map<String, Object>>) responseBody.get("data");

            List<Map<String, Object>> simplifiedPipelines = new ArrayList<>();
            for (Map<String, Object> pipeline : pipelines) {
                Map<String, Object> pipelineInfo = new LinkedHashMap<>();
                pipelineInfo.put("name", pipeline.get("name"));
                pipelineInfo.put("displayName", pipeline.get("displayName"));
                pipelineInfo.put("pipelineType", pipeline.get("pipelineType"));
                pipelineInfo.put("fullyQualifiedName", pipeline.get("fullyQualifiedName"));
                simplifiedPipelines.add(pipelineInfo);
            }

            result.put("success", true);
            result.put("serviceName", serviceName);
            result.put("pipelines", simplifiedPipelines);
            result.put("count", simplifiedPipelines.size());

            if (simplifiedPipelines.isEmpty()) {
                result.put("warning", "No ingestion pipeline found. Please create one in OpenMetadata UI.");
            }

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("Error getting pipelines", e);
            result.put("success", false);
            result.put("error", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }

    @PostMapping("/refresh-all")
    public ResponseEntity<Map<String, Object>> refreshAllIcebergServices(
            @RequestHeader(value = "Authorization", required = false) String authHeader) {
        Map<String, Object> result = new LinkedHashMap<>();
        List<Map<String, String>> triggered = new ArrayList<>();

        try {
            HttpHeaders headers = createHeadersFromAuth(authHeader);
            HttpEntity<String> entity = new HttpEntity<>(headers);

            ResponseEntity<Map> response = restTemplate.exchange(
                openMetadataUrl + "/v1/services/databaseServices?serviceType=Iceberg&limit=100",
                HttpMethod.GET,
                entity,
                Map.class
            );

            Map<String, Object> responseBody = response.getBody();
            List<Map<String, Object>> services = (List<Map<String, Object>>) responseBody.get("data");

            for (Map<String, Object> service : services) {
                String serviceName = (String) service.get("name");
                try {
                    String pipelineName = findIngestionPipeline(serviceName, authHeader);
                    if (pipelineName != null) {
                        triggerIngestion(pipelineName, authHeader);
                        Map<String, String> info = new LinkedHashMap<>();
                        info.put("service", serviceName);
                        info.put("pipeline", pipelineName);
                        info.put("status", "triggered");
                        triggered.add(info);
                    }
                } catch (Exception e) {
                    log.warn("Failed to trigger ingestion for {}: {}", serviceName, e.getMessage());
                }
            }

            result.put("success", true);
            result.put("triggeredCount", triggered.size());
            result.put("triggeredServices", triggered);
            result.put("message", "Metadata refresh triggered for " + triggered.size() + " Iceberg service(s)");

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("Error refreshing all services", e);
            result.put("success", false);
            result.put("error", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }

    @PostMapping("/push-lineage")
    public ResponseEntity<Map<String, Object>> pushLineageToOpenMetadata(
            @RequestHeader(value = "Authorization", required = false) String authHeader,
            @RequestParam(defaultValue = "iceberg-demo") String serviceName) {
        Map<String, Object> result = new LinkedHashMap<>();

        try {
            log.info("Pushing lineage edges to OpenMetadata for service: {}", serviceName);

            String stagingFqn = serviceName + ".demo.staging.orders";
            String productionFqn = serviceName + ".demo.production.orders_clean";
            String analyticsFqn = serviceName + ".demo.analytics.daily_summary";

            addLineageEdge(authHeader, stagingFqn, productionFqn);
            Thread.sleep(500);

            addLineageEdge(authHeader, productionFqn, analyticsFqn);

            result.put("success", true);
            result.put("message", "Lineage edges created successfully!");
            result.put("edges", List.of(
                Map.of("from", stagingFqn, "to", productionFqn),
                Map.of("from", productionFqn, "to", analyticsFqn)
            ));
            result.put("nextSteps", List.of(
                "Go to http://localhost:8585/explore/tables",
                "Search for any table (staging.orders, orders_clean, daily_summary)",
                "Click on table → Lineage tab to see the flow"
            ));

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("Error pushing lineage", e);
            result.put("success", false);
            result.put("error", e.getMessage());
            result.put("hint", "Make sure tables exist in OpenMetadata (run ingestion first) and you have valid token");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }

    private void createNamespaces() {
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.staging");
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.production");
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.analytics");
    }

    private void createStagingTable() {
        spark.sql("""
            CREATE TABLE IF NOT EXISTS demo.staging.orders (
                order_id BIGINT,
                customer_id BIGINT,
                order_time TIMESTAMP,
                amount DECIMAL(10,2),
                status STRING,
                created_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(order_time))
        """);

        spark.sql("""
            INSERT OVERWRITE demo.staging.orders VALUES
            (1, 101, TIMESTAMP '2026-02-01 10:00:00', 100.50, 'completed', current_timestamp()),
            (2, 102, TIMESTAMP '2026-02-01 11:00:00', 200.00, 'completed', current_timestamp()),
            (3, 103, TIMESTAMP '2026-02-02 09:00:00', 150.75, 'pending', current_timestamp()),
            (4, 101, TIMESTAMP '2026-02-02 14:00:00', 300.00, 'completed', current_timestamp()),
            (5, 104, TIMESTAMP '2026-02-03 08:00:00', 50.00, 'cancelled', current_timestamp()),
            (6, 102, TIMESTAMP '2026-02-03 10:00:00', 175.25, 'completed', current_timestamp())
        """);
    }

    private void transformToProduction() {
        Dataset<Row> stagingData = spark.table("demo.staging.orders");

        stagingData
                .filter(col("status").equalTo("completed"))
                .filter(col("amount").gt(0))
                .withColumn("processed_at", current_timestamp())
                .writeTo("demo.production.orders_clean")
                .using("iceberg")
                .createOrReplace();
    }

    private void createAnalytics() {
        spark.sql("""
            CREATE OR REPLACE TABLE demo.analytics.daily_summary
            USING iceberg
            AS
            SELECT 
                DATE(order_time) as order_date,
                COUNT(*) as total_orders,
                SUM(amount) as total_revenue,
                AVG(amount) as avg_order_value,
                COUNT(DISTINCT customer_id) as unique_customers
            FROM demo.production.orders_clean
            GROUP BY DATE(order_time)
            ORDER BY order_date
        """);
    }

    private TableDataResponse convertToTableData(String tableName, Dataset<Row> df) {
        List<String> columns = Arrays.asList(df.columns());

        List<Map<String, Object>> rows = Arrays.stream((Row[]) df.collect())
                .map(row -> {
                    Map<String, Object> rowMap = new HashMap<>();
                    for (int i = 0; i < row.size(); i++) {
                        rowMap.put(columns.get(i), row.get(i));
                    }
                    return rowMap;
                })
                .collect(Collectors.toList());

        return TableDataResponse.builder()
                .tableName(tableName)
                .columns(columns)
                .rows(rows)
                .rowCount(rows.size())
                .build();
    }


    private String findIngestionPipeline(String serviceName, String authHeader) {
        try {
            HttpHeaders headers = createHeadersFromAuth(authHeader);
            HttpEntity<String> entity = new HttpEntity<>(headers);

            ResponseEntity<Map> response = restTemplate.exchange(
                openMetadataUrl + "/v1/services/ingestionPipelines?service=" + serviceName + "&pipelineType=metadata&limit=10",
                HttpMethod.GET,
                entity,
                Map.class
            );

            Map<String, Object> responseBody = response.getBody();
            List<Map<String, Object>> pipelines = (List<Map<String, Object>>) responseBody.get("data");

            if (pipelines != null && !pipelines.isEmpty()) {
                return (String) pipelines.get(0).get("fullyQualifiedName");
            }

            return null;

        } catch (Exception e) {
            log.error("Error finding ingestion pipeline for service: {}", serviceName, e);
            return null;
        }
    }

    private void triggerIngestion(String pipelineFullyQualifiedName) {
        triggerIngestion(pipelineFullyQualifiedName, null);
    }

    private void triggerIngestion(String pipelineFullyQualifiedName, String authHeader) {
        try {
            HttpHeaders headers = createHeadersFromAuth(authHeader);
            HttpEntity<String> entity = new HttpEntity<>("{}", headers);

            restTemplate.exchange(
                openMetadataUrl + "/v1/services/ingestionPipelines/trigger/" + pipelineFullyQualifiedName,
                HttpMethod.POST,
                entity,
                String.class
            );

            log.info("Successfully triggered ingestion pipeline: {}", pipelineFullyQualifiedName);

        } catch (Exception e) {
            log.error("Error triggering ingestion pipeline: {}", pipelineFullyQualifiedName, e);
            throw new RuntimeException("Failed to trigger ingestion: " + e.getMessage(), e);
        }
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

    private void addLineageEdge(String authHeader, String fromTableFqn, String toTableFqn) {
        try {
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
                """, fromTableFqn, toTableFqn);

            HttpHeaders headers = createHeadersFromAuth(authHeader);
            HttpEntity<String> entity = new HttpEntity<>(lineagePayload, headers);

            restTemplate.exchange(
                openMetadataUrl + "/v1/lineage",
                HttpMethod.PUT,
                entity,
                String.class
            );

            log.info("Added lineage edge: {} -> {}", fromTableFqn, toTableFqn);

        } catch (Exception e) {
            log.error("Error adding lineage edge: {}", e.getMessage());
            throw new RuntimeException("Failed to add lineage: " + e.getMessage(), e);
        }
    }
}