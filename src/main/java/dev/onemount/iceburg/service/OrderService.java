package dev.onemount.iceburg.service;

import dev.onemount.iceburg.dto.request.BatchOrderRequest;
import dev.onemount.iceburg.dto.request.CreateOrderRequest;
import dev.onemount.iceburg.dto.response.ExpireSnapshotsResponse;
import dev.onemount.iceburg.dto.request.MergeOrderRequest;
import dev.onemount.iceburg.dto.response.OrderResponse;
import dev.onemount.iceburg.dto.request.QueryOrderRequest;
import dev.onemount.iceburg.dto.SnapshotInfo;
import dev.onemount.iceburg.dto.request.UpdateOrderRequest;
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

        List<SnapshotInfo> beforeSnapshots = getSnapshotHistory();
        int beforeCount = beforeSnapshots.size();

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

        List<SnapshotInfo> beforeSnapshots = getSnapshotHistory();
        int beforeCount = beforeSnapshots.size();

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

}
