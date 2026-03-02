package dev.onemount.iceburg.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DbtFlowResponse {
    private Boolean success;
    private String message;
    private String dagRunId;
    private String dagId;
    private DbtFlowDetails details;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DbtFlowDetails {
        private String sourceTable;
        private String stagingModel;
        private String martModel;
        private String sourceBucket;
        private String targetBucket;
        private String lineageStatus;
        private String airflowUrl;
        private String openMetadataUrl;
    }
}

