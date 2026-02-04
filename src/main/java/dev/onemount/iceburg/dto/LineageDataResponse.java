package dev.onemount.iceburg.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LineageDataResponse {
    private TableDataResponse staging;
    private TableDataResponse production;
    private TableDataResponse analytics;
    private String lineageFlow;
}