package dev.onemount.iceburg.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class QueryOrderRequest {
    private String customerId;
    private String productName;
    private Integer minQuantity;
    private Integer maxQuantity;
    private String startDate;
    private String endDate;

}
