package dev.onemount.iceburg.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class SparkConfig {

    @Value("${spark.app.name}")
    private String appName;

    @Value("${spark.master}")
    private String master;

    @Value("${spark.sql.catalog.demo.uri}")
    private String metastoreUri;

    @Value("${spark.sql.catalog.demo.warehouse}")
    private String warehouse;

    @Value("${iceberg.s3.endpoint}")
    private String s3Endpoint;

    @Value("${iceberg.s3.access-key}")
    private String s3AccessKey;

    @Value("${iceberg.s3.secret-key}")
    private String s3SecretKey;

    @Bean
    public SparkSession sparkSession() {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster(master)
                .set("spark.ui.enabled", "false")
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.hive.HiveCatalog")
                .set("spark.sql.catalog.demo.uri", metastoreUri)
                .set("spark.sql.catalog.demo.warehouse", warehouse)
                .set("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .set("spark.sql.catalog.demo.s3.endpoint", s3Endpoint)
                .set("spark.sql.catalog.demo.s3.access-key-id", s3AccessKey)
                .set("spark.sql.catalog.demo.s3.secret-access-key", s3SecretKey)
                .set("spark.sql.catalog.demo.s3.path-style-access", "true")
                .set("spark.sql.catalog.demo.s3.ssl-enabled", "false")
                .set("spark.hadoop.iceberg.engine.hive.lock-enabled", "false")
                .set("iceberg.engine.hive.lock-enabled", "false")
                .set("spark.sql.defaultCatalog", "demo");

        return SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
