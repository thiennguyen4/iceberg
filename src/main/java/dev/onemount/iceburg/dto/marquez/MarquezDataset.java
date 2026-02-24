package dev.onemount.iceburg.dto.marquez;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MarquezDataset {
    private DatasetId id;
    private String name;
    private String namespace;
    private String type;
    private String physicalName;
    private String sourceName;
    private String createdAt;
    private String updatedAt;
    private List<Object> fields;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DatasetId {
        private String namespace;
        private String name;
    }
}

