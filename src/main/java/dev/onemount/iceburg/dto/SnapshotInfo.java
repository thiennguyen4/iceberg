package dev.onemount.iceburg.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SnapshotInfo {
    private Long snapshotId;
    private String committedAt;
    private String operation;
}
