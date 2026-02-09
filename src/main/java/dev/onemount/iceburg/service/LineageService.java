package dev.onemount.iceburg.service;

import dev.onemount.iceburg.dto.response.LineageDataResponse;
import dev.onemount.iceburg.dto.response.LineageFlowResponse;
import dev.onemount.iceburg.dto.response.TableDataResponse;
import dev.onemount.iceburg.repository.LineageTableRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class LineageService {

    private final LineageTableRepository lineageTableRepository;
    private final OpenMetadataService openMetadataService;

    public LineageFlowResponse createDemoLineageFlow(String authHeader, String serviceName) {
        try {
            lineageTableRepository.createNamespaces();
            lineageTableRepository.createStagingTable();
            lineageTableRepository.transformToProduction();
            lineageTableRepository.createAnalytics();

            String stagingFqn = serviceName + ".default.staging.orders";
            String productionFqn = serviceName + ".default.production.orders_clean";
            String analyticsFqn = serviceName + ".default.analytics.daily_summary";

            try {
                log.info("Triggering OpenMetadata ingestion for service: {}", serviceName);
                String pipelineId = openMetadataService.triggerIngestion(authHeader, serviceName);

                log.info("Waiting for tables to be discovered by OpenMetadata...");
                boolean stagingFound = openMetadataService.waitForTableDiscovery(authHeader, stagingFqn, 30, 2000);
                boolean productionFound = openMetadataService.waitForTableDiscovery(authHeader, productionFqn, 30, 2000);
                boolean analyticsFound = openMetadataService.waitForTableDiscovery(authHeader, analyticsFqn, 30, 2000);

                if (!stagingFound || !productionFound || !analyticsFound) {
                    String notFound = "";
                    if (!stagingFound) notFound += stagingFqn + " ";
                    if (!productionFound) notFound += productionFqn + " ";
                    if (!analyticsFound) notFound += analyticsFqn + " ";

                    throw new RuntimeException("Timeout waiting for tables to be discovered: " + notFound);
                }

                log.info("All tables discovered successfully. Pushing lineage edges...");
                openMetadataService.pushLineageEdge(authHeader, stagingFqn, productionFqn);
                Thread.sleep(500);
                openMetadataService.pushLineageEdge(authHeader, productionFqn, analyticsFqn);

                return LineageFlowResponse.builder()
                        .success(true)
                        .message("Tables and lineage created successfully!")
                        .flowDetails(LineageFlowResponse.LineageFlowDetails.builder()
                                .stagingTable("demo.staging.orders")
                                .productionTable("demo.production.orders_clean")
                                .analyticsTable("demo.analytics.daily_summary")
                                .lineageStatus("Complete: Tables created, ingestion triggered, and lineage relationships established")
                                .build())
                        .build();

            } catch (Exception lineageError) {
                log.warn("Tables created but lineage push failed: {}", lineageError.getMessage());

                return LineageFlowResponse.builder()
                        .success(true)
                        .message("Tables created successfully but lineage setup failed: " + lineageError.getMessage())
                        .flowDetails(LineageFlowResponse.LineageFlowDetails.builder()
                                .stagingTable("demo.staging.orders")
                                .productionTable("demo.production.orders_clean")
                                .analyticsTable("demo.analytics.daily_summary")
                                .lineageStatus("Tables OK, Lineage failed: " + lineageError.getMessage())
                                .build())
                        .build();
            }

        } catch (Exception e) {
            log.error("Error creating lineage flow", e);
            throw new RuntimeException("Failed to create lineage: " + e.getMessage(), e);
        }
    }

    public LineageDataResponse getLineageData() {
        TableDataResponse staging = convertToTableData(
                "iceburg.default.staging.\"staging.orders\"",
                lineageTableRepository.queryTable("demo.staging.orders")
        );

        TableDataResponse production = convertToTableData(
                "iceburg.default.production.\"production.orders_clean\"",
                lineageTableRepository.queryTable("demo.production.orders_clean")
        );

        TableDataResponse analytics = convertToTableData(
                "iceburg.default.analytics.\"analytics.daily_summary\"",
                lineageTableRepository.queryTable("demo.analytics.daily_summary")
        );

        return LineageDataResponse.builder()
                .staging(staging)
                .production(production)
                .analytics(analytics)
                .lineageFlow("staging.orders → production.orders_clean → analytics.daily_summary")
                .build();
    }

    public Map<String, Object> pushLineageEdges(String authHeader, String serviceName) {
        Map<String, Object> result = new LinkedHashMap<>();

        try {
            log.info("Pushing lineage edges to OpenMetadata for service: {}", serviceName);

            String stagingFqn = serviceName + ".demo.staging.orders";
            String productionFqn = serviceName + ".demo.production.orders_clean";
            String analyticsFqn = serviceName + ".demo.analytics.daily_summary";

            openMetadataService.pushLineageEdge(authHeader, stagingFqn, productionFqn);
            Thread.sleep(500);
            openMetadataService.pushLineageEdge(authHeader, productionFqn, analyticsFqn);

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

            return result;

        } catch (Exception e) {
            log.error("Error pushing lineage", e);
            throw new RuntimeException("Failed to push lineage: " + e.getMessage(), e);
        }
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


}
