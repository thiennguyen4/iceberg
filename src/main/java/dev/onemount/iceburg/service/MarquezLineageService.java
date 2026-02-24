package dev.onemount.iceburg.service;

import dev.onemount.iceburg.dto.marquez.MarquezJob;
import dev.onemount.iceburg.dto.marquez.MarquezJobsResponse;
import dev.onemount.iceburg.dto.marquez.MarquezDataset;
import dev.onemount.iceburg.dto.openmetadata.LineageEdge;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class MarquezLineageService {


    private final RestTemplate restTemplate;

    @Value("${marquez.url:http://localhost:5000}")
    private String marquezUrl;

    @Value("${marquez.namespace:iceberg_demo}")
    private String marquezNamespace;

    @Value("${openmetadata.api.url}")
    private String openMetadataUrl;

    public MarquezLineageService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public List<MarquezJob> getMarquezJobs() {
        try {
            String url = String.format("%s/api/v1/namespaces/%s/jobs", marquezUrl, marquezNamespace);
            log.info("Fetching jobs from Marquez: {}", url);

            ResponseEntity<MarquezJobsResponse> response = restTemplate.getForEntity(
                url,
                MarquezJobsResponse.class
            );

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                List<MarquezJob> jobs = response.getBody().getJobs();
                log.info("Found {} jobs in Marquez namespace: {}",
                    jobs != null ? jobs.size() : 0, marquezNamespace);
                return jobs != null ? jobs : new ArrayList<>();
            }

            log.warn("Failed to fetch jobs from Marquez: {}", response.getStatusCode());
            return new ArrayList<>();

        } catch (Exception e) {
            log.error("Error fetching jobs from Marquez", e);
            return new ArrayList<>();
        }
    }

    public MarquezJob getJobDetails(String jobName) {
        try {
            String url = String.format("%s/api/v1/namespaces/%s/jobs/%s",
                marquezUrl, marquezNamespace, jobName);

            log.debug("Fetching job details from Marquez: {}", url);

            ResponseEntity<MarquezJob> response = restTemplate.getForEntity(url, MarquezJob.class);

            if (response.getStatusCode() == HttpStatus.OK) {
                return response.getBody();
            }

            log.warn("Failed to fetch job details: {}", response.getStatusCode());
            return null;

        } catch (Exception e) {
            log.error("Error fetching job details for: {}", jobName, e);
            return null;
        }
    }

    public List<LineageEdge> transformToOpenMetadataLineage(MarquezJob job) {
        List<LineageEdge> edges = new ArrayList<>();

        if (job.getInputs() == null || job.getOutputs() == null) {
            log.debug("Job {} has no inputs or outputs", job.getName());
            return edges;
        }

        for (MarquezDataset input : job.getInputs()) {
            for (MarquezDataset output : job.getOutputs()) {
                LineageEdge edge = LineageEdge.builder()
                    .edge(LineageEdge.Edge.builder()
                        .fromEntity(LineageEdge.EntityReference.builder()
                            .id(input.getName())
                            .type("table")
                            .build())
                        .toEntity(LineageEdge.EntityReference.builder()
                            .id(output.getName())
                            .type("table")
                            .build())
                        .lineageDetails(LineageEdge.LineageDetails.builder()
                            .source("Marquez")
                            .description(String.format("Job: %s", job.getName()))
                            .build())
                        .build())
                    .build();

                edges.add(edge);
                log.debug("Created lineage edge: {} -> {}", input.getName(), output.getName());
            }
        }

        return edges;
    }

    public boolean pushToOpenMetadata(LineageEdge edge) {
        try {
            String url = openMetadataUrl + "/v1/lineage";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<LineageEdge> request = new HttpEntity<>(edge, headers);

            log.debug("Pushing lineage to OpenMetadata: {}", url);

            ResponseEntity<String> response = restTemplate.exchange(
                url,
                HttpMethod.PUT,
                request,
                String.class
            );

            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("Successfully pushed lineage to OpenMetadata");
                return true;
            } else {
                log.warn("Failed to push lineage: {} - {}",
                    response.getStatusCode(), response.getBody());
                return false;
            }

        } catch (Exception e) {
            log.error("Error pushing lineage to OpenMetadata", e);
            return false;
        }
    }

    public void syncLineage() {
        log.info("========================================");
        log.info("Starting Marquez → OpenMetadata lineage sync");
        log.info("Marquez URL: {}", marquezUrl);
        log.info("Namespace: {}", marquezNamespace);
        log.info("OpenMetadata URL: {}", openMetadataUrl);
        log.info("========================================");

        List<MarquezJob> jobs = getMarquezJobs();

        if (jobs.isEmpty()) {
            log.info("No jobs found in Marquez. Skipping sync.");
            return;
        }

        int successCount = 0;
        int failCount = 0;

        for (MarquezJob job : jobs) {
            log.info("Processing job: {}", job.getName());

            MarquezJob jobDetails = getJobDetails(job.getName());

            if (jobDetails == null) {
                log.warn("Could not fetch details for job: {}", job.getName());
                failCount++;
                continue;
            }

            List<LineageEdge> edges = transformToOpenMetadataLineage(jobDetails);

            if (edges.isEmpty()) {
                log.debug("No lineage edges generated for job: {}", job.getName());
                continue;
            }

            for (LineageEdge edge : edges) {
                boolean success = pushToOpenMetadata(edge);
                if (success) {
                    successCount++;
                } else {
                    failCount++;
                }
            }
        }

        log.info("========================================");
        log.info("Lineage sync completed");
        log.info("Success: {}, Failed: {}", successCount, failCount);
        log.info("========================================");
    }
}

