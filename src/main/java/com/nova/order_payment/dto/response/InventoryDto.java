package com.nova.order_payment.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InventoryDto {

    private UUID itemId;
    private String name;
    private BigDecimal price;
    private Double availableQuantity;
    private LocalDate nearestExpiry;
}
