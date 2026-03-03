from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("seed-hive-orders")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.hive.HiveCatalog")
    .config("spark.sql.catalog.demo.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/")
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.demo.s3.access-key-id", "admin")
    .config("spark.sql.catalog.demo.s3.secret-access-key", "password")
    .config("spark.sql.catalog.demo.s3.path-style-access", "true")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.defaultCatalog", "demo")
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.sales")

spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.sales.orders (
        order_id    BIGINT,
        customer_id BIGINT,
        order_time  TIMESTAMP,
        amount      DECIMAL(10, 2),
        status      STRING,
        created_at  TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(order_time))
""")

spark.sql("""
    INSERT OVERWRITE demo.sales.orders VALUES
    (1, 101, TIMESTAMP '2026-02-01 10:00:00', 100.50, 'completed', current_timestamp()),
    (2, 102, TIMESTAMP '2026-02-01 11:00:00', 200.00, 'completed', current_timestamp()),
    (3, 103, TIMESTAMP '2026-02-02 09:00:00', 150.75, 'pending',   current_timestamp()),
    (4, 101, TIMESTAMP '2026-02-02 14:00:00', 300.00, 'completed', current_timestamp()),
    (5, 104, TIMESTAMP '2026-02-03 08:00:00',  50.00, 'cancelled', current_timestamp()),
    (6, 102, TIMESTAMP '2026-02-03 10:00:00', 175.25, 'completed', current_timestamp()),
    (7, 105, TIMESTAMP '2026-02-03 12:00:00', 225.00, 'completed', current_timestamp()),
    (8, 103, TIMESTAMP '2026-02-04 09:30:00',  80.00, 'pending',   current_timestamp())
""")

print("Seeded demo.sales.orders → s3://warehouse/sales/ successfully")
spark.stop()
