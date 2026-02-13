package dev.onemount.iceburg.dto.response;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GenerateBulkDataResponse {
    private Boolean success;
    private String message;
    private Long totalRowsGenerated;
    private Long snapshotId;
    private Long executionTimeMs;
    private String storageLocation;
    private Long fileSizeBytes;
}
