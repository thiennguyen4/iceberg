package dev.onemount.iceburg.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderResponse {
    private String orderId;
    private String customerId;
    private String productName;
    private Integer quantity;
    private BigDecimal price;
    private String orderTimestamp;
}
