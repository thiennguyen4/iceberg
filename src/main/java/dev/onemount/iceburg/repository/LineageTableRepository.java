package dev.onemount.iceburg.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Repository;

import static org.apache.spark.sql.functions.*;

@Slf4j
@Repository
@RequiredArgsConstructor
public class LineageTableRepository {

    private final SparkSession spark;

    public void createNamespaces() {
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.staging");
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.production");
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.analytics");
    }

    public void createStagingTable() {
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

    public void transformToProduction() {
        Dataset<Row> stagingData = spark.table("demo.staging.orders");

        stagingData
                .filter(col("status").equalTo("completed"))
                .filter(col("amount").gt(0))
                .withColumn("processed_at", current_timestamp())
                .writeTo("demo.production.orders_clean")
                .using("iceberg")
                .createOrReplace();
    }

    public void createAnalytics() {
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
        """);
    }

    public Dataset<Row> queryTable(String tableName) {
        return spark.sql("SELECT * FROM " + tableName);
    }
}
