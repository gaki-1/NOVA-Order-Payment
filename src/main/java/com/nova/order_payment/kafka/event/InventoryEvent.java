package com.nova.order_payment.kafka.event;

import com.nova.order_payment.domain.InventoryType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.util.UUID;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InventoryEvent {

    private InventoryEventType type;

    // Full inventory entity snapshot
    private UUID inventoryId;
    private UUID itemId;
    private Double quantity;
    private InventoryType inventoryType;
    private LocalDate expiryDate;
    private UUID referenceKey;
}
