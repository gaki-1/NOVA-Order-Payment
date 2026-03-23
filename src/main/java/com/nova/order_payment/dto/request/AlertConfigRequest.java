package com.nova.order_payment.dto.request;

import com.nova.order_payment.domain.AlertType;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AlertConfigRequest {

    @NotNull
    private UUID itemId;

    @NotNull
    @Min(1)
    private Integer threshold;

    @NotNull
    private AlertType type;
}
