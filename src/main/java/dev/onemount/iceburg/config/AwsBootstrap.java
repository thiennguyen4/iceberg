package dev.onemount.iceburg.config;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AwsBootstrap {

    @Value("${iceberg.s3.region:us-east-1}")
    private String region;

    @Value("${iceberg.s3.access-key}")
    private String accessKey;

    @Value("${iceberg.s3.secret-key}")
    private String secretKey;

    @PostConstruct
    public void init() {
        System.setProperty("aws.region", region);
        System.setProperty("aws.accessKeyId", accessKey);
        System.setProperty("aws.secretAccessKey", secretKey);
        System.setProperty("aws.defaultRegion", region);
    }
}

