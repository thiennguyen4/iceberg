package dev.onemount.iceburg.controller;

import dev.onemount.iceburg.dto.*;
import dev.onemount.iceburg.dto.request.*;
import dev.onemount.iceburg.dto.response.*;
import dev.onemount.iceburg.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    // Create Order
    @PostMapping
    public ResponseEntity<CreateOrderResponse> createOrder(@RequestBody CreateOrderRequest request) {
        log.info("Received create order request: {}", request);

        try {
            Long snapshotId = orderService.createOrder(request);

            CreateOrderResponse response = new CreateOrderResponse(
                    request.getOrderId(),
                    snapshotId,
                    "Order created successfully and stored in Iceberg table"
            );

            log.info("Order created successfully: {}", response);
            return ResponseEntity.status(HttpStatus.CREATED).body(response);

        } catch (Exception e) {
            log.error("Error creating order", e);
            CreateOrderResponse errorResponse = new CreateOrderResponse(
                    request.getOrderId(),
                    null,
                    "Failed to create order: " + e.getMessage()
            );
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    // Health Check
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Order API is running");
    }

    @GetMapping
    public ResponseEntity<List<OrderResponse>> getAllOrders() {
        log.info("Received get all orders request");
        try {
            List<OrderResponse> orders = orderService.getAllOrdersAsList();
            return ResponseEntity.ok(orders);
        } catch (Exception e) {
            log.error("Error getting orders", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // Update Order
    @PutMapping("/{orderId}")
    public ResponseEntity<Map<String, Object>> updateOrder(
            @PathVariable String orderId,
            @RequestBody UpdateOrderRequest request) {
        log.info("Received update order request for ID: {}", orderId);
        try {
            Long snapshotId = orderService.updateOrder(orderId, request);
            return ResponseEntity.ok(Map.of(
                "orderId", orderId,
                "snapshotId", snapshotId,
                "message", "Order updated successfully"
            ));
        } catch (Exception e) {
            log.error("Error updating order", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to update order: " + e.getMessage()));
        }
    }

    // Delete Order
    @DeleteMapping("/{orderId}")
    public ResponseEntity<Map<String, Object>> deleteOrder(@PathVariable String orderId) {
        log.info("Received delete order request for ID: {}", orderId);
        try {
            Long snapshotId = orderService.deleteOrder(orderId);
            return ResponseEntity.ok(Map.of(
                "orderId", orderId,
                "snapshotId", snapshotId,
                "message", "Order deleted successfully"
            ));
        } catch (Exception e) {
            log.error("Error deleting order", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to delete order: " + e.getMessage()));
        }
    }

    // Get Snapshot History
    @GetMapping("/snapshots")
    public ResponseEntity<List<SnapshotInfo>> getSnapshotHistory() {
        log.info("Received get snapshot history request");
        try {
            List<SnapshotInfo> snapshots = orderService.getSnapshotHistory();
            return ResponseEntity.ok(snapshots);
        } catch (Exception e) {
            log.error("Error getting snapshot history", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // Time Travel: Get Orders at Specific Snapshot
    @GetMapping("/time-travel/snapshot/{snapshotId}")
    public ResponseEntity<List<OrderResponse>> getOrdersAtSnapshot(@PathVariable Long snapshotId) {
        log.info("Received time travel request for snapshot: {}", snapshotId);
        try {
            List<OrderResponse> orders = orderService.getOrdersAtSnapshot(snapshotId);
            return ResponseEntity.ok(orders);
        } catch (Exception e) {
            log.error("Error getting orders at snapshot", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // Time Travel: Get Orders at Specific Timestamp
    @GetMapping("/time-travel/timestamp/{timestamp}")
    public ResponseEntity<List<OrderResponse>> getOrdersAtTimestamp(@PathVariable String timestamp) {
        log.info("Received time travel request for timestamp: {}", timestamp);
        try {
            List<OrderResponse> orders = orderService.getOrdersAtTimestamp(timestamp);
            return ResponseEntity.ok(orders);
        } catch (Exception e) {
            log.error("Error getting orders at timestamp", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // Query Orders with Filters
    @PostMapping("/query")
    public ResponseEntity<List<OrderResponse>> queryOrders(@RequestBody QueryOrderRequest request) {
        log.info("Received query orders request: {}", request);
        try {
            List<OrderResponse> orders = orderService.queryOrders(request);
            return ResponseEntity.ok(orders);
        } catch (Exception e) {
            log.error("Error querying orders", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // Merge Order
    @PostMapping("/merge")
    public ResponseEntity<Map<String, Object>> mergeOrder(@RequestBody MergeOrderRequest request) {
        log.info("Received merge order request: {}", request);
        try {
            Long snapshotId = orderService.mergeOrder(request);
            return ResponseEntity.ok(Map.of(
                "orderId", request.getOrderId(),
                "snapshotId", snapshotId,


                "message", "Order merged successfully"
            ));
        } catch (Exception e) {
            log.error("Error merging order", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to merge order: " + e.getMessage()));
        }
    }

    // Batch Ingest Orders
    @PostMapping("/batch")
    public ResponseEntity<Map<String, Object>> batchIngestOrders(@RequestBody BatchOrderRequest request) {
        log.info("Received batch ingest request for {} orders", request.getOrders().size());
        try {
            Long snapshotId = orderService.batchIngestOrders(request);
            return ResponseEntity.ok(Map.of(
                    "message", "Batch ingest completed",
                    "recordsProcessed", request.getOrders().size(),
                    "snapshotId", snapshotId
            ));
        } catch (Exception e) {
            log.error("Error during batch ingestion", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to batch ingest orders: " + e.getMessage()));
        }
    }

    // Expire Snapshots by Time
    @PostMapping("/snapshots/expire/by-time")
    public ResponseEntity<ExpireSnapshotsResponse> expireSnapshotsByTime(@RequestBody ExpireSnapshotsByTimeRequest request) {
        log.info("Received expire snapshots by time request: older than {}", request.getOlderThanTimestamp());
        try {
            ExpireSnapshotsResponse response = orderService.expireSnapshotsByTime(request.getOlderThanTimestamp());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error expiring snapshots by time", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ExpireSnapshotsResponse("Failed to expire snapshots: " + e.getMessage(), 0, 0));
        }
    }

    // Expire Snapshots by Count
    @PostMapping("/snapshots/expire/by-count")
    public ResponseEntity<ExpireSnapshotsResponse> expireSnapshotsByCount(@RequestBody ExpireSnapshotsByCountRequest request) {
        log.info("Received expire snapshots by count request: retain last {}", request.getRetainLast());
        try {
            ExpireSnapshotsResponse response = orderService.expireSnapshotsByCount(request.getRetainLast());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error expiring snapshots by count", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ExpireSnapshotsResponse("Failed to expire snapshots: " + e.getMessage(), 0, 0));
        }
    }

    // Expire Snapshots Hybrid (time and count)
    @PostMapping("/snapshots/expire/hybrid")
    public ResponseEntity<ExpireSnapshotsResponse> expireSnapshotsHybrid(@RequestBody ExpireSnapshotsHybridRequest request) {
        log.info("Received expire snapshots hybrid request: older than {}, retain last {}",
                request.getOlderThanTimestamp(), request.getRetainLast());
        try {
            ExpireSnapshotsResponse response = orderService.expireSnapshotsHybrid(
                    request.getOlderThanTimestamp(),
                    request.getRetainLast()
            );
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error expiring snapshots with hybrid strategy", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ExpireSnapshotsResponse("Failed to expire snapshots: " + e.getMessage(), 0, 0));
        }
    }

    @PostMapping("/generate-bulk-data")
    public ResponseEntity<GenerateBulkDataResponse> generateBulkData(@RequestBody GenerateBulkDataRequest request) {
        log.info("Received bulk data generation request: {} rows",
                request.getRowCount() != null ? request.getRowCount() : 1000000);
        try {
            GenerateBulkDataResponse response = orderService.generateBulkData(request);

            if (response.getSuccess()) {
                return ResponseEntity.ok(response);
            } else {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
            }
        } catch (Exception e) {
            log.error("Error generating bulk data", e);
            GenerateBulkDataResponse errorResponse = GenerateBulkDataResponse.builder()
                    .success(false)
                    .message("Failed to generate bulk data: " + e.getMessage())
                    .build();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @PostMapping("/batch-parallel")
    public ResponseEntity<ParallelBatchOrderResponse> parallelBatchIngestOrders(
            @RequestBody ParallelBatchOrderRequest request) {
        log.info("Received parallel batch ingest request: {} orders with {} threads",
                request.getOrders().size(),
                request.getNumberOfThreads() != null ? request.getNumberOfThreads() : 4);
        try {
            ParallelBatchOrderResponse response = orderService.parallelBatchIngestOrders(request);

            if (response.getSuccess()) {
                return ResponseEntity.ok(response);
            } else {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
            }
        } catch (Exception e) {
            log.error("Error during parallel batch ingestion", e);
            ParallelBatchOrderResponse errorResponse = ParallelBatchOrderResponse.builder()
                    .success(false)
                    .message("Failed to parallel batch ingest orders: " + e.getMessage())
                    .build();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @PostMapping("/benchmark/full-scan")
    public ResponseEntity<QueryBenchmarkResponse> benchmarkFullTableScan(
            @RequestBody(required = false) QueryBenchmarkRequest request) {
        if (request == null) {
            request = new QueryBenchmarkRequest();
        }
        log.info("Received full table scan benchmark request: {} iterations", request.getIterations());

        try {
            QueryBenchmarkResponse response = orderService.benchmarkFullTableScan(request);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error during full table scan benchmark", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/benchmark/filtered-query")
    public ResponseEntity<QueryBenchmarkResponse> benchmarkFilteredQuery(
            @RequestBody(required = false) QueryBenchmarkRequest request) {
        if (request == null) {
            request = new QueryBenchmarkRequest();
        }
        log.info("Received filtered query benchmark request: customerId={}, productName={}",
                request.getCustomerId(), request.getProductName());

        try {
            QueryBenchmarkResponse response = orderService.benchmarkFilteredQuery(request);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error during filtered query benchmark", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/benchmark/aggregation")
    public ResponseEntity<QueryBenchmarkResponse> benchmarkAggregationQuery(
            @RequestBody(required = false) QueryBenchmarkRequest request) {
        if (request == null) {
            request = new QueryBenchmarkRequest();
        }
        log.info("Received aggregation query benchmark request: {} iterations", request.getIterations());

        try {
            QueryBenchmarkResponse response = orderService.benchmarkAggregationQuery(request);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error during aggregation query benchmark", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
