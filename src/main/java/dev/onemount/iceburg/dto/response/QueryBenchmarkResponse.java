package dev.onemount.iceburg.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryBenchmarkResponse {
    private String queryType;
    private String description;
    private Integer totalIterations;
    private Long totalRowsScanned;
    private Long totalRowsReturned;
    private Long minExecutionTimeMs;
    private Long maxExecutionTimeMs;
    private Long avgExecutionTimeMs;
    private Long medianExecutionTimeMs;
    private List<Long> executionTimesMs;
    private Double throughputRowsPerSecond;
    private String queryDetails;
}
