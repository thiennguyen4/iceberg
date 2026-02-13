package dev.onemount.iceburg.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ParallelBatchOrderResponse {
    private Boolean success;
    private String message;
    private Integer totalOrders;
    private Integer numberOfThreads;
    private Long totalExecutionTimeMs;
    private Long sequentialExecutionTimeMs;
    private Double speedupFactor;
    private Long snapshotId;
    private List<ThreadExecutionDetail> threadDetails;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class ThreadExecutionDetail {
        private String threadName;
        private Integer ordersProcessed;
        private Long executionTimeMs;
        private String status;
    }
}
