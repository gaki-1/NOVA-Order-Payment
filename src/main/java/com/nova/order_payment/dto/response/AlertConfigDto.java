package com.nova.order_payment.dto.response;

import com.nova.order_payment.domain.AlertType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AlertConfigDto {

    private UUID alertId;
    private UUID itemId;
    private String itemName;
    private Integer threshold;
    private AlertType type;
}
