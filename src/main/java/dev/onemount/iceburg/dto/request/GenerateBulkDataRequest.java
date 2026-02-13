package dev.onemount.iceburg.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GenerateBulkDataRequest {
    private Integer rowCount = 1000000;
    private Integer batchSize = 10000;
}
