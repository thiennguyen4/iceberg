package dev.onemount.iceburg.service;

import dev.onemount.iceburg.dto.request.*;
import dev.onemount.iceburg.dto.response.ExpireSnapshotsResponse;
import dev.onemount.iceburg.dto.response.GenerateBulkDataResponse;
import dev.onemount.iceburg.dto.response.OrderResponse;
import dev.onemount.iceburg.dto.SnapshotInfo;
import dev.onemount.iceburg.dto.response.QueryBenchmarkResponse;
import dev.onemount.iceburg.dto.response.ParallelBatchOrderResponse;
import dev.onemount.iceburg.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;

    public Long createOrder(CreateOrderRequest request) {
        log.info("Creating order with ID: {}", request.getOrderId());

        orderRepository.ensureNamespaceExists();
        orderRepository.ensureTableExists();

        Timestamp orderTimestamp = Timestamp.from(Instant.now());
        orderRepository.insertOrder(
                request.getOrderId(),
                request.getCustomerId(),
                request.getProductName(),
                request.getQuantity(),
                request.getPrice(),
                orderTimestamp
        );

        Long snapshotId = orderRepository.getLatestSnapshotId();
        log.info("Order created successfully. Snapshot ID: {}", snapshotId);

        return snapshotId;
    }

    public List<OrderResponse> getAllOrdersAsList() {
        return orderRepository.findAll();
    }

    public Long updateOrder(String orderId, UpdateOrderRequest request) {
        log.info("Updating order with ID: {}", orderId);

        Map<String, Object> updates = new HashMap<>();

        if (request.getCustomerId() != null) {
            updates.put("customerId", request.getCustomerId());
        }
        if (request.getProductName() != null) {
            updates.put("productName", request.getProductName());
        }
        if (request.getQuantity() != null) {
            updates.put("quantity", request.getQuantity());
        }
        if (request.getPrice() != null) {
            updates.put("price", request.getPrice());
        }

        orderRepository.updateOrder(orderId, updates);
        log.info("Order updated successfully");

        return orderRepository.getLatestSnapshotId();
    }

    public Long deleteOrder(String orderId) {
        log.info("Deleting order with ID: {}", orderId);
        orderRepository.deleteOrder(orderId);
        log.info("Order deleted successfully");
        return orderRepository.getLatestSnapshotId();
    }

    public List<SnapshotInfo> getSnapshotHistory() {
        log.info("Retrieving snapshot history");
        List<SnapshotInfo> snapshots = orderRepository.getSnapshotHistory();
        log.info("Retrieved {} snapshots", snapshots.size());
        return snapshots;
    }

    public List<OrderResponse> getOrdersAtSnapshot(Long snapshotId) {
        log.info("Retrieving orders at snapshot: {}", snapshotId);
        List<OrderResponse> orders = orderRepository.findBySnapshot(snapshotId);
        log.info("Retrieved {} orders at snapshot {}", orders.size(), snapshotId);
        return orders;
    }

    public List<OrderResponse> getOrdersAtTimestamp(String timestamp) {
        log.info("Retrieving orders at timestamp: {}", timestamp);
        List<OrderResponse> orders = orderRepository.findByTimestamp(timestamp);
        log.info("Retrieved {} orders at timestamp {}", orders.size(), timestamp);
        return orders;
    }

    public List<OrderResponse> queryOrders(QueryOrderRequest request) {
        log.info("Querying orders with filters: {}", request);

        List<OrderResponse> orders = orderRepository.findByFilters(
                request.getCustomerId(),
                request.getProductName(),
                request.getMinQuantity(),
                request.getMaxQuantity(),
                request.getStartDate(),
                request.getEndDate()
        );

        log.info("Query returned {} orders", orders.size());
        return orders;
    }


    public Long mergeOrder(MergeOrderRequest request) {
        log.info("Merging order with ID: {}", request.getOrderId());

        orderRepository.ensureNamespaceExists();
        orderRepository.ensureTableExists();

        Timestamp orderTimestamp = Timestamp.from(Instant.now());

        orderRepository.mergeOrder(
                request.getOrderId(),
                request.getCustomerId(),
                request.getProductName(),
                request.getQuantity(),
                request.getPrice(),
                orderTimestamp
        );

        log.info("Order merged successfully using Iceberg MERGE");
        return orderRepository.getLatestSnapshotId();
    }

    public Long batchIngestOrders(BatchOrderRequest request) {
        log.info("Starting batch ingestion for {} orders", request.getOrders().size());

        orderRepository.ensureNamespaceExists();
        orderRepository.ensureTableExists();

        Timestamp orderTimestamp = Timestamp.from(Instant.now());
        List<Map<String, Object>> orderDataList = new ArrayList<>();

        for (BatchOrderRequest.OrderItem item : request.getOrders()) {
            Map<String, Object> orderData = new HashMap<>();
            orderData.put("orderId", item.getOrderId());
            orderData.put("customerId", item.getCustomerId());
            orderData.put("productName", item.getProductName());
            orderData.put("quantity", item.getQuantity());
            orderData.put("price", item.getPrice());
            orderData.put("orderTimestamp", orderTimestamp);
            orderDataList.add(orderData);
        }

        orderRepository.batchInsertOrders(orderDataList);
        log.info("Batch ingestion completed successfully for {} orders", request.getOrders().size());

        return orderRepository.getLatestSnapshotId();
    }

    public ExpireSnapshotsResponse expireSnapshotsByTime(String olderThanTimestamp) {
        log.info("Expiring snapshots older than {}", olderThanTimestamp);

        List<SnapshotInfo> beforeSnapshots = getSnapshotHistory();
        int beforeCount = beforeSnapshots.size();

        int expired = orderRepository.expireSnapshotsByTime(olderThanTimestamp);

        List<SnapshotInfo> afterSnapshots = getSnapshotHistory();
        int afterCount = afterSnapshots.size();

        log.info("Snapshot expiration by time completed: {} expired, {} retained", expired, afterCount);
        return new ExpireSnapshotsResponse(
                "Snapshots expired by time successfully",
                expired,
                afterCount
        );
    }

    public ExpireSnapshotsResponse expireSnapshotsByCount(int retainLast) {
        log.info("Expiring snapshots, retaining last {} snapshots", retainLast);

        int expired = orderRepository.expireSnapshotsByCount(retainLast);

        List<SnapshotInfo> afterSnapshots = getSnapshotHistory();
        int afterCount = afterSnapshots.size();

        log.info("Snapshot expiration by count completed: {} expired, {} retained", expired, afterCount);
        return new ExpireSnapshotsResponse(
                "Snapshots expired by count successfully",
                expired,
                afterCount
        );
    }

    public ExpireSnapshotsResponse expireSnapshotsHybrid(String olderThanTimestamp, int retainLast) {
        log.info("Expiring snapshots older than {}, retaining last {} snapshots", olderThanTimestamp, retainLast);

        int expired = orderRepository.expireSnapshotsHybrid(olderThanTimestamp, retainLast);

        List<SnapshotInfo> afterSnapshots = getSnapshotHistory();
        int afterCount = afterSnapshots.size();

        log.info("Snapshot expiration with hybrid strategy completed: {} expired, {} retained", expired, afterCount);
        return new ExpireSnapshotsResponse(
                "Snapshots expired with hybrid strategy successfully",
                expired,
                afterCount
        );
    }

    public GenerateBulkDataResponse generateBulkData(GenerateBulkDataRequest request) {
        int rowCount = request.getRowCount() != null ? request.getRowCount() : 1000000;
        int batchSize = request.getBatchSize() != null ? request.getBatchSize() : 10000;

        log.info("Starting bulk data generation: {} rows with batch size {}", rowCount, batchSize);

        try {
            Map<String, Object> result = orderRepository.generateBulkData(rowCount, batchSize);

            return GenerateBulkDataResponse.builder()
                    .success(true)
                    .message("Bulk data generation completed successfully")
                    .totalRowsGenerated((Long) result.get("rowsGenerated"))
                    .snapshotId((Long) result.get("snapshotId"))
                    .executionTimeMs((Long) result.get("executionTimeMs"))
                    .storageLocation((String) result.get("storageLocation"))
                    .fileSizeBytes((Long) result.get("fileSizeBytes"))
                    .build();

        } catch (Exception e) {
            log.error("Error during bulk data generation", e);
            return GenerateBulkDataResponse.builder()
                    .success(false)
                    .message("Failed to generate bulk data: " + e.getMessage())
                    .build();
        }
    }

    public QueryBenchmarkResponse benchmarkFullTableScan(QueryBenchmarkRequest request) {
        int iterations = request.getIterations() != null ? request.getIterations() : 5;
        boolean warmup = request.getWarmup() != null ? request.getWarmup() : true;

        log.info("Starting full table scan benchmark: {} iterations, warmup={}", iterations, warmup);

        try {
            Map<String, Object> result = orderRepository.benchmarkFullTableScan(iterations, warmup);

            return QueryBenchmarkResponse.builder()
                    .queryType("FULL_TABLE_SCAN")
                    .description("Benchmark for full table scan query")
                    .totalIterations(iterations)
                    .totalRowsScanned((Long) result.get("totalRowsScanned"))
                    .totalRowsReturned((Long) result.get("totalRowsReturned"))
                    .minExecutionTimeMs((Long) result.get("minExecutionTimeMs"))
                    .maxExecutionTimeMs((Long) result.get("maxExecutionTimeMs"))
                    .avgExecutionTimeMs((Long) result.get("avgExecutionTimeMs"))
                    .medianExecutionTimeMs((Long) result.get("medianExecutionTimeMs"))
                    .executionTimesMs((List<Long>) result.get("executionTimes"))
                    .throughputRowsPerSecond((Double) result.get("throughputRowsPerSecond"))
                    .queryDetails((String) result.get("queryDetails"))
                    .build();

        } catch (Exception e) {
            log.error("Error during full table scan benchmark", e);
            throw new RuntimeException("Failed to benchmark full table scan: " + e.getMessage(), e);
        }
    }

    public QueryBenchmarkResponse benchmarkFilteredQuery(QueryBenchmarkRequest request) {
        int iterations = request.getIterations() != null ? request.getIterations() : 5;
        boolean warmup = request.getWarmup() != null ? request.getWarmup() : true;
        String customerId = request.getCustomerId() != null ? request.getCustomerId() : "CUST-001000";
        String productName = request.getProductName() != null ? request.getProductName() : "Laptop";

        log.info("Starting filtered query benchmark: customerId={}, productName={}, {} iterations",
                customerId, productName, iterations);

        try {
            Map<String, Object> result = orderRepository.benchmarkFilteredQuery(
                    customerId, productName, iterations, warmup);

            return QueryBenchmarkResponse.builder()
                    .queryType("FILTERED_QUERY")
                    .description("Benchmark for filtered query with WHERE clause")
                    .totalIterations(iterations)
                    .totalRowsScanned((Long) result.get("totalRowsScanned"))
                    .totalRowsReturned((Long) result.get("totalRowsReturned"))
                    .minExecutionTimeMs((Long) result.get("minExecutionTimeMs"))
                    .maxExecutionTimeMs((Long) result.get("maxExecutionTimeMs"))
                    .avgExecutionTimeMs((Long) result.get("avgExecutionTimeMs"))
                    .medianExecutionTimeMs((Long) result.get("medianExecutionTimeMs"))
                    .executionTimesMs((List<Long>) result.get("executionTimes"))
                    .throughputRowsPerSecond((Double) result.get("throughputRowsPerSecond"))
                    .queryDetails((String) result.get("queryDetails"))
                    .build();

        } catch (Exception e) {
            log.error("Error during filtered query benchmark", e);
            throw new RuntimeException("Failed to benchmark filtered query: " + e.getMessage(), e);
        }
    }

    public QueryBenchmarkResponse benchmarkAggregationQuery(QueryBenchmarkRequest request) {
        int iterations = request.getIterations() != null ? request.getIterations() : 5;
        boolean warmup = request.getWarmup() != null ? request.getWarmup() : true;

        log.info("Starting aggregation query benchmark: {} iterations, warmup={}", iterations, warmup);

        try {
            Map<String, Object> result = orderRepository.benchmarkAggregationQuery(iterations, warmup);

            return QueryBenchmarkResponse.builder()
                    .queryType("AGGREGATION_QUERY")
                    .description("Benchmark for aggregation query with GROUP BY")
                    .totalIterations(iterations)
                    .totalRowsScanned((Long) result.get("totalRowsScanned"))
                    .totalRowsReturned((Long) result.get("totalRowsReturned"))
                    .minExecutionTimeMs((Long) result.get("minExecutionTimeMs"))
                    .maxExecutionTimeMs((Long) result.get("maxExecutionTimeMs"))
                    .avgExecutionTimeMs((Long) result.get("avgExecutionTimeMs"))
                    .medianExecutionTimeMs((Long) result.get("medianExecutionTimeMs"))
                    .executionTimesMs((List<Long>) result.get("executionTimes"))
                    .throughputRowsPerSecond((Double) result.get("throughputRowsPerSecond"))
                    .queryDetails((String) result.get("queryDetails"))
                    .build();

        } catch (Exception e) {
            log.error("Error during aggregation query benchmark", e);
            throw new RuntimeException("Failed to benchmark aggregation query: " + e.getMessage(), e);
        }
    }

    public ParallelBatchOrderResponse parallelBatchIngestOrders(ParallelBatchOrderRequest request) {
        int numberOfThreads = request.getNumberOfThreads() != null ? request.getNumberOfThreads() : 4;
        int totalOrders = request.getOrders().size();

        log.info("Starting parallel batch ingestion: {} orders with {} threads", totalOrders, numberOfThreads);

        orderRepository.ensureNamespaceExists();
        orderRepository.ensureTableExists();

        long overallStartTime = System.currentTimeMillis();

        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
        List<Future<ParallelBatchOrderResponse.ThreadExecutionDetail>> futures = new ArrayList<>();

        int batchSize = (int) Math.ceil((double) totalOrders / numberOfThreads);
        List<List<ParallelBatchOrderRequest.OrderItem>> partitions = partitionList(request.getOrders(), batchSize);

        for (int i = 0; i < partitions.size(); i++) {
            final int threadIndex = i;
            final List<ParallelBatchOrderRequest.OrderItem> partition = partitions.get(i);

            Future<ParallelBatchOrderResponse.ThreadExecutionDetail> future = executorService.submit(() -> {
                String threadName = "Thread-" + threadIndex;
                long threadStartTime = System.currentTimeMillis();

                try {
                    log.info("{} processing {} orders", threadName, partition.size());

                    Timestamp orderTimestamp = Timestamp.from(Instant.now());
                    List<Map<String, Object>> orderDataList = new ArrayList<>();

                    for (ParallelBatchOrderRequest.OrderItem item : partition) {
                        Map<String, Object> orderData = new HashMap<>();
                        orderData.put("orderId", item.getOrderId());
                        orderData.put("customerId", item.getCustomerId());
                        orderData.put("productName", item.getProductName());
                        orderData.put("quantity", item.getQuantity());
                        orderData.put("price", item.getPrice());
                        orderData.put("orderTimestamp", orderTimestamp);
                        orderDataList.add(orderData);
                    }

                    orderRepository.batchInsertOrders(orderDataList);

                    long threadEndTime = System.currentTimeMillis();
                    long threadExecutionTime = threadEndTime - threadStartTime;

                    log.info("{} completed: {} orders in {} ms", threadName, partition.size(), threadExecutionTime);

                    return ParallelBatchOrderResponse.ThreadExecutionDetail.builder()
                            .threadName(threadName)
                            .ordersProcessed(partition.size())
                            .executionTimeMs(threadExecutionTime)
                            .status("SUCCESS")
                            .build();

                } catch (Exception e) {
                    long threadEndTime = System.currentTimeMillis();
                    log.error("{} failed: {}", threadName, e.getMessage(), e);

                    return ParallelBatchOrderResponse.ThreadExecutionDetail.builder()
                            .threadName(threadName)
                            .ordersProcessed(0)
                            .executionTimeMs(threadEndTime - threadStartTime)
                            .status("FAILED: " + e.getMessage())
                            .build();
                }
            });

            futures.add(future);
        }

        List<ParallelBatchOrderResponse.ThreadExecutionDetail> threadDetails = new ArrayList<>();
        for (Future<ParallelBatchOrderResponse.ThreadExecutionDetail> future : futures) {
            try {
                threadDetails.add(future.get());
            } catch (Exception e) {
                log.error("Error getting thread result", e);
            }
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        long overallEndTime = System.currentTimeMillis();
        long totalExecutionTime = overallEndTime - overallStartTime;

        long estimatedSequentialTime = threadDetails.stream()
                .mapToLong(ParallelBatchOrderResponse.ThreadExecutionDetail::getExecutionTimeMs)
                .sum();

        double speedupFactor = estimatedSequentialTime > 0 ?
                (double) estimatedSequentialTime / totalExecutionTime : 1.0;

        Long snapshotId = orderRepository.getLatestSnapshotId();

        log.info("Parallel batch ingestion completed: {} orders in {} ms (speedup: {}x)",
                totalOrders, totalExecutionTime, String.format("%.2f", speedupFactor));

        return ParallelBatchOrderResponse.builder()
                .success(true)
                .message("Parallel batch ingestion completed successfully")
                .totalOrders(totalOrders)
                .numberOfThreads(partitions.size())
                .totalExecutionTimeMs(totalExecutionTime)
                .sequentialExecutionTimeMs(estimatedSequentialTime)
                .speedupFactor(speedupFactor)
                .snapshotId(snapshotId)
                .threadDetails(threadDetails)
                .build();
    }

    private <T> List<List<T>> partitionList(List<T> list, int batchSize) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            partitions.add(new ArrayList<>(
                    list.subList(i, Math.min(i + batchSize, list.size()))
            ));
        }
        return partitions;
    }

}
