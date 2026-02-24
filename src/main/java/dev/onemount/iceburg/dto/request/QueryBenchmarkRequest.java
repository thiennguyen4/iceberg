package dev.onemount.iceburg.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class QueryBenchmarkRequest {
    private Integer iterations = 5;
    private Boolean warmup = true;
    private String customerId;
    private String productName;
    private Integer limit;
}
