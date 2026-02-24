package dev.onemount.iceburg.dto.openmetadata;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LineageEdge {
    private Edge edge;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Edge {
        private EntityReference fromEntity;
        private EntityReference toEntity;
        private LineageDetails lineageDetails;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EntityReference {
        private String id;
        private String type;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LineageDetails {
        private String source;
        private String description;
        private String sqlQuery;
    }
}

