package dev.onemount.iceburg.repository;

import dev.onemount.iceburg.dto.response.OrderResponse;
import dev.onemount.iceburg.dto.SnapshotInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public interface OrderRepository {

    void ensureNamespaceExists();

    void ensureTableExists();

    void insertOrder(String orderId, String customerId, String productName, Integer quantity, BigDecimal price, Timestamp orderTimestamp);

    void updateOrder(String orderId, Map<String, Object> updates);

    void deleteOrder(String orderId);

    void mergeOrder(String orderId, String customerId, String productName, Integer quantity, BigDecimal price, Timestamp orderTimestamp);

    List<OrderResponse> findAll();

    List<OrderResponse> findByFilters(String customerId, String productName, Integer minQuantity, Integer maxQuantity, String startDate, String endDate);

    List<OrderResponse> findBySnapshot(Long snapshotId);

    List<OrderResponse> findByTimestamp(String timestamp);

    List<SnapshotInfo> getSnapshotHistory();

    Long getLatestSnapshotId();

    Dataset<Row> getAllOrdersDataset();

    void batchInsertOrders(List<Map<String, Object>> orders);

    int expireSnapshotsByTime(String olderThanTimestamp);

    int expireSnapshotsByCount(int retainLast);

    int expireSnapshotsHybrid(String olderThanTimestamp, int retainLast);

    Map<String, Object> generateBulkData(int rowCount, int batchSize);

    Map<String, Object> benchmarkFullTableScan(int iterations, boolean warmup);

    Map<String, Object> benchmarkFilteredQuery(String customerId, String productName, int iterations, boolean warmup);

    Map<String, Object> benchmarkAggregationQuery(int iterations, boolean warmup);
}
