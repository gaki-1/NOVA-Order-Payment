package com.nova.order_payment.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.UUID;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderEvent {

    private UUID orderId;
    private OrderEventType type;
    private List<OrderLineItem> orderLines;

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class OrderLineItem {
        private UUID itemId;
        private int quantity;
    }
}
