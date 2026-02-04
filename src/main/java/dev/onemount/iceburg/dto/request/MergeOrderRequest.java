package dev.onemount.iceburg.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MergeOrderRequest {
    private String orderId;
    private String customerId;
    private String productName;
    private Integer quantity;
    private BigDecimal price;
}
