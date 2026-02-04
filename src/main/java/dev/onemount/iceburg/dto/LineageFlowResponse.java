package dev.onemount.iceburg.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LineageFlowResponse {
    private Boolean success;
    private String message;
    private LineageFlowDetails flowDetails;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LineageFlowDetails {
        private String stagingTable;
        private String productionTable;
        private String analyticsTable;
        private String lineageStatus;
    }
}
