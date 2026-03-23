package com.nova.order_payment.dto.response;

import com.nova.order_payment.domain.OrderStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderDto {

    private UUID orderId;
    private OrderStatus status;
    private List<OrderItemDto> items;
    private BigDecimal totalPrice;
    private LocalDateTime createdAt;

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class OrderItemDto {

        private UUID itemId;
        private String itemName;
        private Integer quantity;
        private BigDecimal priceSnapshot;
    }
}
