package dev.onemount.iceburg.dto.marquez;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MarquezJob {
    private JobId id;
    private String name;
    private String simpleName;
    private String namespace;
    private String type;
    private List<MarquezDataset> inputs;
    private List<MarquezDataset> outputs;
    private String description;
    private String location;
    private String createdAt;
    private String updatedAt;
    private LatestRun latestRun;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JobId {
        private String namespace;
        private String name;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class LatestRun {
        private String id;
        private String state;
        private String createdAt;
        private String updatedAt;
        private String startedAt;
        private String endedAt;
    }
}

