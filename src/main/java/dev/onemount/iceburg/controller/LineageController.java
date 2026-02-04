package dev.onemount.iceburg.controller;

import dev.onemount.iceburg.dto.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

@Slf4j
@RestController
@RequestMapping("/api/lineage")
@RequiredArgsConstructor
public class LineageController {

    private final SparkSession spark;
    private final RestTemplate restTemplate;

    @Value("${openmetadata.api.url}")
    private String openMetadataApiUrl;

    @PostMapping("/demo/create-flow")
    public ResponseEntity<LineageFlowResponse> createDemoLineageFlow(
            @RequestHeader(value = "Authorization", required = false) String authHeader) {
        try {
            createNamespaces();
            createStagingTable();
            transformToProduction();
            createAnalytics();

            publishLineageToOpenMetadata(authHeader);

            LineageFlowResponse response = LineageFlowResponse.builder()
                    .success(true)
                    .message("Lineage flow created successfully")
                    .flowDetails(LineageFlowResponse.LineageFlowDetails.builder()
                            .stagingTable("demo.staging.orders")
                            .productionTable("demo.production.orders_clean")
                            .analyticsTable("demo.analytics.daily_summary")
                            .lineageStatus("Published to OpenMetadata")
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

    private void publishLineageToOpenMetadata(String authHeader) {
        try {
            String stagingId = getTableId("iceburg.default.staging.\"staging.orders\"", authHeader);
            String productionId = getTableId("iceburg.default.production.\"production.orders_clean\"", authHeader);
            String analyticsId = getTableId("iceburg.default.analytics.\"analytics.daily_summary\"", authHeader);

            if (stagingId == null || productionId == null || analyticsId == null) {
                log.warn("Some tables not found in OpenMetadata. Skipping lineage publishing.");
                return;
            }

            addLineageEdge(stagingId, productionId, authHeader);
            addLineageEdge(productionId, analyticsId, authHeader);

            log.info("Lineage published successfully");

        } catch (Exception e) {
            log.error("Failed to publish lineage to OpenMetadata", e);
        }
    }

    private String getTableId(String tableName, String authHeader) {
        try {
            if (authHeader == null || !authHeader.startsWith("Bearer ")) {
                log.warn("Invalid or missing Authorization header");
                return null;
            }

            String token = authHeader.substring(7);
            String url = openMetadataApiUrl + "/v1/tables/name/" + tableName;

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBearerAuth(token);

            HttpEntity<String> entity = new HttpEntity<>(headers);
            ResponseEntity<Map> response = restTemplate.exchange(
                    url, HttpMethod.GET, entity, Map.class
            );

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                return (String) response.getBody().get("id");
            }
        } catch (Exception e) {
            log.debug("Table not found: {}", tableName);
        }
        return null;
    }

    private void addLineageEdge(String fromId, String toId, String authHeader) {
        try {
            if (authHeader == null || !authHeader.startsWith("Bearer ")) {
                log.warn("Invalid or missing Authorization header for lineage edge");
                return;
            }

            String token = authHeader.substring(7);
            String url = openMetadataApiUrl + "/v1/lineage";

            Map<String, Object> payload = Map.of(
                    "edge", Map.of(
                            "fromEntity", Map.of("id", fromId, "type", "table"),
                            "toEntity", Map.of("id", toId, "type", "table"),
                            "lineageDetails", Map.of("sqlQuery", "")
                    )
            );

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBearerAuth(token);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(payload, headers);
            restTemplate.exchange(url, HttpMethod.PUT, entity, String.class);

        } catch (Exception e) {
            log.error("Failed to create lineage edge: {} -> {}", fromId, toId, e);
        }
    }
}