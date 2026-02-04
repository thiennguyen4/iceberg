package dev.onemount.iceburg.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BatchOrderRequest {
    private List<OrderItem> orders;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderItem {
        private String orderId;
        private String customerId;
        private String productName;
        private Integer quantity;
        private BigDecimal price;
    }
}
