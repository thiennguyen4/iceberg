package dev.onemount.iceburg.repository;

import dev.onemount.iceburg.config.IcebergTableConfig;
import dev.onemount.iceburg.dto.response.OrderResponse;
import dev.onemount.iceburg.dto.SnapshotInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
@RequiredArgsConstructor
public class SparkOrderRepository implements OrderRepository {

    private final SparkSession sparkSession;
    private final IcebergTableConfig icebergTableConfig;

    private static final String NAMESPACE = "sales";
    private static final String TABLE_NAME = "orders";
    private static final String FULL_TABLE_NAME = NAMESPACE + "." + TABLE_NAME;

    @Override
    public void ensureNamespaceExists() {
        String createNamespaceSql = String.format(
                "CREATE NAMESPACE IF NOT EXISTS %s",
                NAMESPACE
        );

        log.debug("Executing SQL: {}", createNamespaceSql);
        sparkSession.sql(createNamespaceSql);
        log.info("Namespace '{}' ensured to exist", NAMESPACE);
    }

    @Override
    public void ensureTableExists() {
        String createTableSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "  order_id STRING," +
                        "  customer_id STRING," +
                        "  product_name STRING," +
                        "  quantity INT," +
                        "  price DECIMAL(10,2)," +
                        "  order_timestamp TIMESTAMP" +
                        ") USING iceberg %s",
                FULL_TABLE_NAME,
                icebergTableConfig.getTableProperties()
        );

        log.debug("Executing SQL: {}", createTableSql);
        sparkSession.sql(createTableSql);
        log.info("Table '{}' ensured to exist with format: {}", FULL_TABLE_NAME, icebergTableConfig.getWriteFormat());
    }

    @Override
    public void insertOrder(String orderId, String customerId, String productName, Integer quantity, BigDecimal price, Timestamp orderTimestamp) {
        String insertSql = String.format(
                "INSERT INTO %s VALUES ('%s', '%s', '%s', %d, %s, TIMESTAMP '%s')",
                FULL_TABLE_NAME,
                orderId,
                customerId,
                productName,
                quantity,
                price,
                orderTimestamp.toString()
        );

        log.debug("Executing SQL: {}", insertSql);
        sparkSession.sql(insertSql);
        log.info("Order data inserted into table '{}'", FULL_TABLE_NAME);
    }

    @Override
    public void updateOrder(String orderId, Map<String, Object> updates) {
        StringBuilder updateSql = new StringBuilder(String.format("UPDATE %s SET ", FULL_TABLE_NAME));
        List<String> setStatements = new ArrayList<>();

        if (updates.containsKey("customerId")) {
            setStatements.add(String.format("customer_id = '%s'", updates.get("customerId")));
        }
        if (updates.containsKey("productName")) {
            setStatements.add(String.format("product_name = '%s'", updates.get("productName")));
        }
        if (updates.containsKey("quantity")) {
            setStatements.add(String.format("quantity = %d", updates.get("quantity")));
        }
        if (updates.containsKey("price")) {
            setStatements.add(String.format("price = %s", updates.get("price")));
        }

        updateSql.append(String.join(", ", setStatements));
        updateSql.append(String.format(" WHERE order_id = '%s'", orderId));

        log.debug("Executing SQL: {}", updateSql);
        sparkSession.sql(updateSql.toString());
        log.info("Order updated successfully");
    }

    @Override
    public void deleteOrder(String orderId) {
        String deleteSql = String.format("DELETE FROM %s WHERE order_id = '%s'", FULL_TABLE_NAME, orderId);

        log.debug("Executing SQL: {}", deleteSql);
        sparkSession.sql(deleteSql);
        log.info("Order deleted successfully");
    }

    @Override
    public void mergeOrder(String orderId, String customerId, String productName, Integer quantity, BigDecimal price, Timestamp orderTimestamp) {
        String mergeSql = String.format(
                "MERGE INTO %s AS target " +
                        "USING (SELECT '%s' AS order_id, '%s' AS customer_id, '%s' AS product_name, %d AS quantity, %s AS price, TIMESTAMP '%s' AS order_timestamp) AS source " +
                        "ON target.order_id = source.order_id " +
                        "WHEN MATCHED THEN UPDATE SET " +
                        "  customer_id = source.customer_id, " +
                        "  product_name = source.product_name, " +
                        "  quantity = source.quantity, " +
                        "  price = source.price " +
                        "WHEN NOT MATCHED THEN INSERT (order_id, customer_id, product_name, quantity, price, order_timestamp) " +
                        "VALUES (source.order_id, source.customer_id, source.product_name, source.quantity, source.price, source.order_timestamp)",
                FULL_TABLE_NAME,
                orderId,
                customerId,
                productName,
                quantity,
                price,
                orderTimestamp.toString()
        );

        log.debug("Executing MERGE SQL: {}", mergeSql);
        sparkSession.sql(mergeSql);
        log.info("Order merged successfully using Iceberg MERGE");
    }

    @Override
    public List<OrderResponse> findAll() {
        String selectSql = String.format("SELECT * FROM %s", FULL_TABLE_NAME);
        log.debug("Executing SQL: {}", selectSql);
        Dataset<Row> ordersDs = sparkSession.sql(selectSql);
        return mapRowsToOrderResponses(ordersDs);
    }

    @Override
    public List<OrderResponse> findByFilters(String customerId, String productName, Integer minQuantity, Integer maxQuantity, String startDate, String endDate) {
        Dataset<Row> ordersDs = sparkSession.table(FULL_TABLE_NAME);

        if (customerId != null && !customerId.isEmpty()) {
            ordersDs = ordersDs.filter(String.format("customer_id = '%s'", customerId));
        }
        if (productName != null && !productName.isEmpty()) {
            ordersDs = ordersDs.filter(String.format("product_name = '%s'", productName));
        }
        if (minQuantity != null) {
            ordersDs = ordersDs.filter(String.format("quantity >= %d", minQuantity));
        }
        if (maxQuantity != null) {
            ordersDs = ordersDs.filter(String.format("quantity <= %d", maxQuantity));
        }
        if (startDate != null && !startDate.isEmpty()) {
            ordersDs = ordersDs.filter(String.format("order_timestamp >= '%s'", startDate));
        }
        if (endDate != null && !endDate.isEmpty()) {
            ordersDs = ordersDs.filter(String.format("order_timestamp <= '%s'", endDate));
        }

        return mapRowsToOrderResponses(ordersDs);
    }

    @Override
    public List<OrderResponse> findBySnapshot(Long snapshotId) {
        String timeTravelSql = String.format(
                "SELECT * FROM %s VERSION AS OF %d",
                FULL_TABLE_NAME,
                snapshotId
        );

        log.debug("Executing SQL: {}", timeTravelSql);
        Dataset<Row> ordersDs = sparkSession.sql(timeTravelSql);
        return mapRowsToOrderResponses(ordersDs);
    }

    @Override
    public List<OrderResponse> findByTimestamp(String timestamp) {
        String timeTravelSql = String.format(
                "SELECT * FROM %s TIMESTAMP AS OF '%s'",
                FULL_TABLE_NAME,
                timestamp
        );

        log.debug("Executing SQL: {}", timeTravelSql);
        Dataset<Row> ordersDs = sparkSession.sql(timeTravelSql);
        return mapRowsToOrderResponses(ordersDs);
    }

    @Override
    public List<SnapshotInfo> getSnapshotHistory() {
        String snapshotsSql = String.format(
                "SELECT snapshot_id, committed_at, operation FROM %s.snapshots ORDER BY committed_at DESC",
                FULL_TABLE_NAME
        );

        Dataset<Row> snapshotsDs = sparkSession.sql(snapshotsSql);
        List<SnapshotInfo> snapshots = new ArrayList<>();

        snapshotsDs.collectAsList().forEach(row -> {
            snapshots.add(new SnapshotInfo(
                    row.getLong(row.fieldIndex("snapshot_id")),
                    row.getTimestamp(row.fieldIndex("committed_at")).toString(),
                    row.getString(row.fieldIndex("operation"))
            ));
        });

        return snapshots;
    }

    @Override
    public Long getLatestSnapshotId() {
        try {
            String snapshotsSql = String.format("SELECT snapshot_id FROM %s.snapshots ORDER BY committed_at DESC LIMIT 1", FULL_TABLE_NAME);
            Dataset<Row> snapshotDs = sparkSession.sql(snapshotsSql);

            if (!snapshotDs.isEmpty()) {
                Long snapshotId = snapshotDs.first().getLong(0);
                log.debug("Current snapshot ID: {}", snapshotId);
                return snapshotId;
            } else {
                log.warn("No snapshots found for table '{}'", FULL_TABLE_NAME);
                return null;
            }
        } catch (Exception e) {
            log.error("Error retrieving snapshot ID", e);
            return null;
        }
    }

    @Override
    public Dataset<Row> getAllOrdersDataset() {
        String selectSql = String.format("SELECT * FROM %s", FULL_TABLE_NAME);
        log.debug("Executing SQL: {}", selectSql);
        return sparkSession.sql(selectSql);
    }

    private List<OrderResponse> mapRowsToOrderResponses(Dataset<Row> ordersDs) {
        List<OrderResponse> orders = new ArrayList<>();

        ordersDs.collectAsList().forEach(row -> {
            int quantityIndex = row.fieldIndex("quantity");
            int priceIndex = row.fieldIndex("price");
            orders.add(new OrderResponse(
                    row.getString(row.fieldIndex("order_id")),
                    row.getString(row.fieldIndex("customer_id")),
                    row.getString(row.fieldIndex("product_name")),
                    row.isNullAt(quantityIndex) ? null : row.getInt(quantityIndex),
                    row.isNullAt(priceIndex) ? null : row.getDecimal(priceIndex),
                    row.getTimestamp(row.fieldIndex("order_timestamp")).toString()
            ));
        });

        return orders;
    }

    @Override
    public void batchInsertOrders(List<Map<String, Object>> orders) {
        if (orders == null || orders.isEmpty()) {
            log.warn("No orders to insert");
            return;
        }

        try {
            StructType schema = new StructType()
                    .add("order_id", DataTypes.StringType, false)
                    .add("customer_id", DataTypes.StringType, false)
                    .add("product_name", DataTypes.StringType, false)
                    .add("quantity", DataTypes.IntegerType, false)
                    .add("price", DataTypes.createDecimalType(10, 2), false)
                    .add("order_timestamp", DataTypes.TimestampType, false);

            List<Row> rows = new ArrayList<>();
            for (Map<String, Object> order : orders) {
                rows.add(RowFactory.create(
                        order.get("orderId"),
                        order.get("customerId"),
                        order.get("productName"),
                        order.get("quantity"),
                        order.get("price"),
                        order.get("orderTimestamp")
                ));
            }

            Dataset<Row> df = sparkSession.createDataFrame(rows, schema);
            df.writeTo(FULL_TABLE_NAME).append();

            log.info("Batch inserted {} orders into table '{}'", orders.size(), FULL_TABLE_NAME);
        } catch (Exception e) {
            log.error("Error during batch insert", e);
            throw new RuntimeException("Failed to batch insert orders", e);
        }
    }

    @Override
    public int expireSnapshotsByTime(String olderThanTimestamp) {
        try {
            log.info("Expiring snapshots older than {}", olderThanTimestamp);

            List<SnapshotInfo> beforeSnapshots = getSnapshotHistory();
            int beforeCount = beforeSnapshots.size();

            String expireSql = String.format(
                    "CALL system.expire_snapshots(table => '%s.%s', older_than => TIMESTAMP '%s')",
                    NAMESPACE,
                    TABLE_NAME,
                    olderThanTimestamp
            );

            log.info("Executing SQL: {}", expireSql);
            sparkSession.sql(expireSql);

            List<SnapshotInfo> afterSnapshots = getSnapshotHistory();
            int afterCount = afterSnapshots.size();
            int expired = beforeCount - afterCount;

            log.info("Expired {} snapshots", expired);
            return expired;
        } catch (Exception e) {
            log.error("Error expiring snapshots by time", e);
            throw new RuntimeException("Failed to expire snapshots by time", e);
        }
    }

    @Override
    public int expireSnapshotsByCount(int retainLast) {
        try {
            log.info("Expiring snapshots, retaining last {} snapshots", retainLast);

            List<SnapshotInfo> beforeSnapshots = getSnapshotHistory();
            int beforeCount = beforeSnapshots.size();

            if (beforeCount <= retainLast) {
                log.info("No snapshots to expire. Total: {}, Retain: {}", beforeCount, retainLast);
                return 0;
            }

            String expireSql = String.format(
                    "CALL system.expire_snapshots(table => '%s.%s', retain_last => %d)",
                    NAMESPACE,
                    TABLE_NAME,
                    retainLast
            );

            log.info("Executing SQL: {}", expireSql);
            sparkSession.sql(expireSql);

            List<SnapshotInfo> afterSnapshots = getSnapshotHistory();
            int afterCount = afterSnapshots.size();
            int expired = beforeCount - afterCount;

            log.info("Expired {} snapshots, retained {} snapshots", expired, afterCount);
            return expired;
        } catch (Exception e) {
            log.error("Error expiring snapshots by count", e);
            throw new RuntimeException("Failed to expire snapshots by count", e);
        }
    }


    @Override
    public int expireSnapshotsHybrid(String olderThanTimestamp, int retainLast) {
        try {
            log.info("Expiring snapshots older than {}, but retaining last {} snapshots", olderThanTimestamp, retainLast);

            List<SnapshotInfo> beforeSnapshots = getSnapshotHistory();
            int beforeCount = beforeSnapshots.size();

            String expireSql = String.format(
                    "CALL system.expire_snapshots(table => '%s.%s', older_than => TIMESTAMP '%s', retain_last => %d)",
                    NAMESPACE,
                    TABLE_NAME,
                    olderThanTimestamp,
                    retainLast
            );

            log.info("Executing SQL: {}", expireSql);
            sparkSession.sql(expireSql);

            List<SnapshotInfo> afterSnapshots = getSnapshotHistory();
            int afterCount = afterSnapshots.size();
            int expired = beforeCount - afterCount;

            log.info("Expired {} snapshots, retained {} snapshots", expired, afterCount);
            return expired;
        } catch (Exception e) {
            log.error("Error expiring snapshots with hybrid strategy", e);
            throw new RuntimeException("Failed to expire snapshots with hybrid strategy", e);
        }
    }

    @Override
    public Map<String, Object> generateBulkData(int rowCount, int batchSize) {
        long startTime = System.currentTimeMillis();

        try {
            log.info("Starting bulk data generation: {} rows with batch size {}", rowCount, batchSize);

            ensureNamespaceExists();
            ensureTableExists();

            String generateSql = String.format(
                "INSERT INTO %s " +
                "SELECT " +
                "  CONCAT('ORD-', LPAD(CAST(id AS STRING), 10, '0')) as order_id, " +
                "  CONCAT('CUST-', LPAD(CAST((id %% 10000) AS STRING), 6, '0')) as customer_id, " +
                "  CASE (id %% 10) " +
                "    WHEN 0 THEN 'Laptop' " +
                "    WHEN 1 THEN 'Smartphone' " +
                "    WHEN 2 THEN 'Tablet' " +
                "    WHEN 3 THEN 'Monitor' " +
                "    WHEN 4 THEN 'Keyboard' " +
                "    WHEN 5 THEN 'Mouse' " +
                "    WHEN 6 THEN 'Headphones' " +
                "    WHEN 7 THEN 'Webcam' " +
                "    WHEN 8 THEN 'Speaker' " +
                "    ELSE 'Charger' " +
                "  END as product_name, " +
                "  CAST((id %% 10 + 1) AS INT) as quantity, " +
                "  CAST(((id %% 1000 + 100) * 1.99) AS DECIMAL(10,2)) as price, " +
                "  TIMESTAMP(DATE_ADD(CURRENT_DATE(), -CAST((id %% 365) AS INT))) as order_timestamp " +
                "FROM RANGE(%d)",
                FULL_TABLE_NAME,
                rowCount
            );

            log.info("Executing bulk insert SQL");
            sparkSession.sql(generateSql);

            long endTime = System.currentTimeMillis();
            long executionTime = endTime - startTime;

            Long snapshotId = getLatestSnapshotId();

            String location = "s3://warehouse/" + NAMESPACE + "/" + TABLE_NAME;

            long totalSize = 0;
            try {
                String filesSql = String.format(
                    "SELECT SUM(file_size_in_bytes) as total_size FROM %s.files",
                    FULL_TABLE_NAME
                );
                Dataset<Row> filesDs = sparkSession.sql(filesSql);
                if (!filesDs.isEmpty() && !filesDs.first().isNullAt(0)) {
                    totalSize = filesDs.first().getLong(0);
                }
            } catch (Exception e) {
                log.warn("Could not calculate total file size: {}", e.getMessage());
            }

            Map<String, Object> result = new java.util.HashMap<>();
            result.put("rowsGenerated", (long) rowCount);
            result.put("snapshotId", snapshotId);
            result.put("executionTimeMs", executionTime);
            result.put("storageLocation", location);
            result.put("fileSizeBytes", totalSize);

            log.info("Bulk data generation completed: {} rows in {} ms", rowCount, executionTime);

            return result;

        } catch (Exception e) {
            log.error("Error generating bulk data", e);
            throw new RuntimeException("Failed to generate bulk data", e);
        }
    }

    @Override
    public Map<String, Object> benchmarkFullTableScan(int iterations, boolean warmup) {
        List<Long> executionTimes = new ArrayList<>();

        try {
            if (warmup) {
                log.info("Performing warmup query...");
                sparkSession.sql(String.format("SELECT COUNT(*) FROM %s", FULL_TABLE_NAME)).collect();
            }

            String querySql = String.format("SELECT * FROM %s", FULL_TABLE_NAME);
            long totalRows = 0;

            for (int i = 0; i < iterations; i++) {
                long startTime = System.currentTimeMillis();

                Dataset<Row> result = sparkSession.sql(querySql);
                long count = result.count();

                long endTime = System.currentTimeMillis();
                long executionTime = endTime - startTime;
                executionTimes.add(executionTime);

                if (i == 0) {
                    totalRows = count;
                }

                log.info("Iteration {}: {} rows in {} ms", i + 1, count, executionTime);
            }

            return buildBenchmarkResult(executionTimes, totalRows, totalRows,
                "Full table scan - SELECT * FROM " + FULL_TABLE_NAME);

        } catch (Exception e) {
            log.error("Error during full table scan benchmark", e);
            throw new RuntimeException("Failed to benchmark full table scan", e);
        }
    }

    @Override
    public Map<String, Object> benchmarkFilteredQuery(String customerId, String productName,
                                                      int iterations, boolean warmup) {
        List<Long> executionTimes = new ArrayList<>();

        try {
            if (warmup) {
                log.info("Performing warmup query...");
                String warmupSql = String.format(
                    "SELECT * FROM %s WHERE customer_id = '%s' AND product_name = '%s'",
                    FULL_TABLE_NAME, customerId, productName
                );
                sparkSession.sql(warmupSql).collect();
            }

            String querySql = String.format(
                "SELECT * FROM %s WHERE customer_id = '%s' AND product_name = '%s'",
                FULL_TABLE_NAME, customerId, productName
            );

            long totalRowsScanned = sparkSession.sql(String.format("SELECT COUNT(*) FROM %s", FULL_TABLE_NAME))
                .first().getLong(0);
            long filteredRows = 0;

            for (int i = 0; i < iterations; i++) {
                long startTime = System.currentTimeMillis();

                Dataset<Row> result = sparkSession.sql(querySql);
                long count = result.count();

                long endTime = System.currentTimeMillis();
                long executionTime = endTime - startTime;
                executionTimes.add(executionTime);

                if (i == 0) {
                    filteredRows = count;
                }

                log.info("Iteration {}: {} rows filtered in {} ms", i + 1, count, executionTime);
            }

            return buildBenchmarkResult(executionTimes, totalRowsScanned, filteredRows,
                String.format("Filtered query - WHERE customer_id='%s' AND product_name='%s'",
                    customerId, productName));

        } catch (Exception e) {
            log.error("Error during filtered query benchmark", e);
            throw new RuntimeException("Failed to benchmark filtered query", e);
        }
    }

    @Override
    public Map<String, Object> benchmarkAggregationQuery(int iterations, boolean warmup) {
        List<Long> executionTimes = new ArrayList<>();

        try {
            if (warmup) {
                log.info("Performing warmup query...");
                sparkSession.sql(String.format(
                    "SELECT product_name, COUNT(*) as count, SUM(quantity) as total_qty, AVG(price) as avg_price FROM %s GROUP BY product_name",
                    FULL_TABLE_NAME
                )).collect();
            }

            String querySql = String.format(
                "SELECT product_name, COUNT(*) as order_count, " +
                "SUM(quantity) as total_quantity, " +
                "AVG(price) as avg_price, " +
                "MIN(price) as min_price, " +
                "MAX(price) as max_price " +
                "FROM %s GROUP BY product_name ORDER BY order_count DESC",
                FULL_TABLE_NAME
            );

            long totalRowsScanned = sparkSession.sql(String.format("SELECT COUNT(*) FROM %s", FULL_TABLE_NAME))
                .first().getLong(0);
            long resultRows = 0;

            for (int i = 0; i < iterations; i++) {
                long startTime = System.currentTimeMillis();

                Dataset<Row> result = sparkSession.sql(querySql);
                long count = result.count();

                long endTime = System.currentTimeMillis();
                long executionTime = endTime - startTime;
                executionTimes.add(executionTime);

                if (i == 0) {
                    resultRows = count;
                }

                log.info("Iteration {}: {} groups aggregated in {} ms", i + 1, count, executionTime);
            }

            return buildBenchmarkResult(executionTimes, totalRowsScanned, resultRows,
                "Aggregation query - GROUP BY product_name with COUNT, SUM, AVG, MIN, MAX");

        } catch (Exception e) {
            log.error("Error during aggregation query benchmark", e);
            throw new RuntimeException("Failed to benchmark aggregation query", e);
        }
    }

    private Map<String, Object> buildBenchmarkResult(List<Long> executionTimes, long rowsScanned,
                                                     long rowsReturned, String queryDetails) {
        Map<String, Object> result = new java.util.HashMap<>();

        executionTimes.sort(Long::compareTo);

        long min = executionTimes.get(0);
        long max = executionTimes.get(executionTimes.size() - 1);
        long sum = executionTimes.stream().mapToLong(Long::longValue).sum();
        long avg = sum / executionTimes.size();
        long median = executionTimes.get(executionTimes.size() / 2);

        double throughput = rowsScanned > 0 && avg > 0 ? (rowsScanned * 1000.0 / avg) : 0.0;

        result.put("executionTimes", executionTimes);
        result.put("minExecutionTimeMs", min);
        result.put("maxExecutionTimeMs", max);
        result.put("avgExecutionTimeMs", avg);
        result.put("medianExecutionTimeMs", median);
        result.put("totalRowsScanned", rowsScanned);
        result.put("totalRowsReturned", rowsReturned);
        result.put("throughputRowsPerSecond", throughput);
        result.put("queryDetails", queryDetails);

        return result;
    }
}
