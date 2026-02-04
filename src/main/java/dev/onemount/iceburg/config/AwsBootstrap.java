package dev.onemount.iceburg.config;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

@Component
public class AwsBootstrap {

    @PostConstruct
    public void init() {
        System.setProperty("aws.region", "us-east-1");
        System.setProperty("aws.accessKeyId", "minioadmin");
        System.setProperty("aws.secretAccessKey", "minioadmin");
    }
}

