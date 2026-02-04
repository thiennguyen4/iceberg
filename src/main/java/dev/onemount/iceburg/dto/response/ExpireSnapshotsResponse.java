package dev.onemount.iceburg.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExpireSnapshotsResponse {
    private String message;
    private Integer snapshotsExpired;
    private Integer snapshotsRetained;
}
