package dev.onemount.iceburg.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExpireSnapshotsByTimeRequest {
    private String olderThanTimestamp;
}
