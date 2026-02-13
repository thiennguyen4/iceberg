package dev.onemount.iceburg.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MarquezLineageResponse {
    private Boolean success;
    private String message;
    private String marquezUrl;
    private List<String> capturedEvents;
    private String jobName;
}