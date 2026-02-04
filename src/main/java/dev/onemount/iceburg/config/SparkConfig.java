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

    @Value("${spark.sql.catalog.demo.write.format.default:parquet}")
    private String defaultFileFormat;

    @Value("${spark.sql.catalog.demo.write.parquet.compression-codec:zstd}")
    private String parquetCompression;

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
                .set("spark.sql.catalog.demo.warehouse", warehouse)
                .set("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .set("spark.sql.catalog.demo.s3.endpoint", s3Endpoint)
                .set("spark.sql.catalog.demo.s3.access-key-id", s3AccessKey)
                .set("spark.sql.catalog.demo.s3.secret-access-key", s3SecretKey)
                .set("spark.sql.catalog.demo.s3.path-style-access", "true")
                .set("spark.sql.defaultCatalog", "demo")

                // S3A Configuration
                .set("spark.hadoop.fs.s3a.endpoint", s3Endpoint)
                .set("spark.hadoop.fs.s3a.access.key", s3AccessKey)
                .set("spark.hadoop.fs.s3a.secret.key", s3SecretKey)
                .set("spark.hadoop.fs.s3a.path.style.access", "true")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

//                // OpenMetadata Configuration
//                .set("spark.openmetadata.host", "http://openmetadata:8585")
//                .set("spark.openmetadata.auth.type", "no-auth")

                // Write Configuration
                .set("spark.sql.catalog.demo.write.format.default", defaultFileFormat)
                .set("spark.sql.catalog.demo.write.parquet.compression-codec", parquetCompression);


        return SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
