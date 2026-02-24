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
    private String catalogUri;

    @Value("${spark.sql.catalog.demo.warehouse}")
    private String warehouse;

    @Value("${spark.sql.catalog.demo.s3.endpoint}")
    private String s3Endpoint;

    @Value("${spark.sql.catalog.demo.s3.access-key-id}")
    private String s3AccessKey;

    @Value("${spark.sql.catalog.demo.s3.secret-access-key}")
    private String s3SecretKey;

    @Bean
    public SparkSession sparkSession() {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster(master)
                .set("spark.ui.enabled", "false")

                // Iceberg Configuration
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.demo.type", "rest")
                .set("spark.sql.catalog.demo.uri", catalogUri)
                .set("spark.sql.catalog.demo.warehouse", warehouse) // Point to S3 bucket
                .set("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") // Use S3FileIO for S3 access

                // S3 Configuration for Iceberg
                .set("spark.sql.catalog.demo.s3.endpoint", s3Endpoint)
                .set("spark.sql.catalog.demo.s3.access-key-id", s3AccessKey)
                .set("spark.sql.catalog.demo.s3.secret-access-key", s3SecretKey)
                .set("spark.sql.catalog.demo.s3.path-style-access", "true")
                .set("spark.sql.defaultCatalog", "demo")
                .set("spark.hadoop.fs.s3a.endpoint", s3Endpoint)
                .set("spark.hadoop.fs.s3a.access.key", s3AccessKey)
                .set("spark.hadoop.fs.s3a.secret.key", s3SecretKey)
                .set("spark.hadoop.fs.s3a.path.style.access", "true")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .set("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
                .set("spark.openlineage.transport.type", "http")
                .set("spark.openlineage.transport.url", "http://localhost:5002")
                .set("spark.openlineage.namespace", "iceberg_demo")
                .set("spark.openlineage.parentJobNamespace", "iceberg_demo")
                .set("spark.openlineage.parentJobName", "iceberg_lineage_job");


        return SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
