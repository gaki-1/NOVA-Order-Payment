package com.nova.order_payment.kafka.consumer;

import com.nova.order_payment.domain.AlertConfig;
import com.nova.order_payment.kafka.event.OrderEvent;
import com.nova.order_payment.kafka.event.OrderEventType;
import com.nova.order_payment.repository.AlertConfigRepository;
import com.nova.order_payment.repository.InventoryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final InventoryRepository inventoryRepository;
    private final AlertConfigRepository alertConfigRepository;

    @KafkaListener(topics = "${kafka.topic.order-updates}", groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderEvent(OrderEvent event) {
        log.info("OrderEvent received: type={} orderId={}", event.getType(), event.getOrderId());
        if (event.getType() == OrderEventType.ORDER_PLACED) {
            event.getOrderLines().forEach(line -> checkAlerts(line.getItemId(), event.getOrderId()));
        }
    }

    private void checkAlerts(UUID itemId, UUID orderId) {
        LocalDate today = LocalDate.now();
        double supply = inventoryRepository.sumSupplyQuantity(itemId, today);
        double demand = inventoryRepository.sumDemandQuantity(itemId);
        double available = Math.max(0, supply - demand);

        alertConfigRepository.findAllByItem_Id(itemId).forEach(config -> {
            if (available <= config.getThreshold()) {
                triggerAlert(config, itemId, available, orderId);
            }
        });
    }

    private void triggerAlert(AlertConfig config, UUID itemId, double available, UUID orderId) {
        log.warn("ALERT [{}] itemId={} available={} threshold={} triggeredByOrder={}",
                config.getType(), itemId, available, config.getThreshold(), orderId);
    }
}
