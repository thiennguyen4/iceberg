package dev.onemount.iceburg.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableDataResponse {
    private String tableName;
    private List<String> columns;
    private List<Map<String, Object>> rows;
    private Integer rowCount;
}