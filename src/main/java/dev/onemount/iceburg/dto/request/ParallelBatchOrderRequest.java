package dev.onemount.iceburg.dto.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ParallelBatchOrderRequest {
    private List<OrderItem> orders;
    private Integer numberOfThreads;
    private Integer batchSizePerThread;

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
