package dev.onemount.iceburg.dto.marquez;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MarquezJobsResponse {
    private List<MarquezJob> jobs;
    private Integer totalCount;
}

