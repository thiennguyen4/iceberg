package dev.onemount.iceburg.service;

import dev.onemount.iceburg.dto.response.DbtFlowResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class DbtService {

    private final RestTemplate restTemplate;

    @Value("${airflow.api.url:http://localhost:8080/api/v2}")
    private String airflowApiUrl;

    @Value("${airflow.username:admin}")
    private String airflowUsername;

    @Value("${airflow.password:admin}")
    private String airflowPassword;

    private static final String DAG_ID = "dbt_hive_transform_lineage";

    public DbtFlowResponse triggerDbtFlow() {
        HttpHeaders headers = buildAirflowHeaders();
        String runId = "dbt_nessie_run_" + System.currentTimeMillis();

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("dag_run_id", runId);
        payload.put("conf", Map.of());

        restTemplate.exchange(
                airflowApiUrl + "/dags/" + DAG_ID + "/dagRuns",
                HttpMethod.POST,
                new HttpEntity<>(payload, headers),
                String.class
        );

        log.info("Triggered dbt Hive DAG: dagRunId={}", runId);

        return DbtFlowResponse.builder()
                .success(true)
                .message("dbt DAG triggered. Flow: seed → stg_orders → mart_daily_revenue → OpenMetadata lineage.")
                .dagRunId(runId)
                .dagId(DAG_ID)
                .details(DbtFlowResponse.DbtFlowDetails.builder()
                        .sourceTable("demo.sales.orders")
                        .stagingModel("demo.dbt_staging.stg_orders")
                        .martModel("demo.dbt_mart.mart_daily_revenue")
                        .sourceBucket("s3://warehouse/sales/")
                        .targetBucket("s3://warehouse/dbt_mart/")
                        .lineageStatus("Pending - running in Airflow")
                        .airflowUrl("http://localhost:8080/dags/dbt_hive_transform_lineage")
                        .openMetadataUrl("http://localhost:8585/explore/tables")
                        .build())
                .build();
    }

    public Map<String, Object> getDagRunStatus(String dagRunId) {
        HttpEntity<Void> request = new HttpEntity<>(buildAirflowHeaders());

        ResponseEntity<Map<String, Object>> response = restTemplate.exchange(
                airflowApiUrl + "/dags/" + DAG_ID + "/dagRuns/" + dagRunId,
                HttpMethod.GET,
                request,
                new ParameterizedTypeReference<>() {}
        );

        return response.getBody();
    }

    private HttpHeaders buildAirflowHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBasicAuth(airflowUsername, airflowPassword);
        return headers;
    }
}




