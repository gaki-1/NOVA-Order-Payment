package com.nova.order_payment.kafka.producer;

import com.nova.order_payment.kafka.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.order-updates}")
    private String orderUpdatesTopic;

    public void publishOrderEvent(OrderEvent event) {
        kafkaTemplate.send(orderUpdatesTopic, String.valueOf(event.getOrderId()), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish OrderEvent type={} orderId={}", event.getType(), event.getOrderId(), ex);
                    }
                });
    }
}
