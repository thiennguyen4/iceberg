from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("seed-nessie-orders")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    )
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v2")
    .config("spark.sql.catalog.nessie.ref", "main")
    .config("spark.sql.catalog.nessie.warehouse", "s3://nessie/")
    .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.nessie.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.nessie.s3.access-key-id", "admin")
    .config("spark.sql.catalog.nessie.s3.secret-access-key", "password")
    .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.default")

spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.default.orders (
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
    INSERT OVERWRITE nessie.default.orders VALUES
    (1, 101, TIMESTAMP '2026-02-01 10:00:00', 100.50, 'completed', current_timestamp()),
    (2, 102, TIMESTAMP '2026-02-01 11:00:00', 200.00, 'completed', current_timestamp()),
    (3, 103, TIMESTAMP '2026-02-02 09:00:00', 150.75, 'pending',   current_timestamp()),
    (4, 101, TIMESTAMP '2026-02-02 14:00:00', 300.00, 'completed', current_timestamp()),
    (5, 104, TIMESTAMP '2026-02-03 08:00:00',  50.00, 'cancelled', current_timestamp()),
    (6, 102, TIMESTAMP '2026-02-03 10:00:00', 175.25, 'completed', current_timestamp()),
    (7, 105, TIMESTAMP '2026-02-03 12:00:00', 225.00, 'completed', current_timestamp()),
    (8, 103, TIMESTAMP '2026-02-04 09:30:00',  80.00, 'pending',   current_timestamp())
""")

print("Seeded nessie.default.orders → s3://nessie/default/ successfully")
spark.stop()

