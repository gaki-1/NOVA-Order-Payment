package com.nova.order_payment.kafka.producer;

import com.nova.order_payment.kafka.event.InventoryEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.inventory-updates}")
    private String inventoryUpdatesTopic;

    public void publishInventoryEvent(InventoryEvent event) {
        kafkaTemplate.send(inventoryUpdatesTopic, String.valueOf(event.getItemId()), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish InventoryEvent type={} itemId={}", event.getType(), event.getItemId(), ex);
                    }
                });
    }
}
