package com.nova.order_payment.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nova.order_payment.domain.StockTransaction;
import com.nova.order_payment.kafka.event.InventoryEvent;
import com.nova.order_payment.repository.StockTransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class StockTransactionConsumer {

    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    private final StockTransactionRepository stockTransactionRepository;

    @KafkaListener(topics = "${kafka.topic.inventory-updates}", groupId = "stock-transaction-group")
    public void onInventoryEvent(InventoryEvent event) {
        log.info("InventoryEvent received: type={} itemId={} qty={}", event.getType(), event.getItemId(), event.getQuantity());

        try {
            String payload = MAPPER.writeValueAsString(event);
            stockTransactionRepository.save(StockTransaction.builder()
                    .eventType(event.getType().name())
                    .payload(payload)
                    .build());
            log.debug("StockTransaction saved: eventType={}", event.getType());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize InventoryEvent to JSON: type={} itemId={}", event.getType(), event.getItemId(), e);
        }
    }
}
