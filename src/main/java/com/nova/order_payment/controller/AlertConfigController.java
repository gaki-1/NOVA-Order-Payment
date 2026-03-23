package com.nova.order_payment.controller;

import com.nova.order_payment.dto.request.AlertConfigRequest;
import com.nova.order_payment.dto.response.AlertConfigDto;
import com.nova.order_payment.service.AlertConfigService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/inventory/alerts")
@RequiredArgsConstructor
public class AlertConfigController {

    private final AlertConfigService alertConfigService;

    @PostMapping
    public ResponseEntity<AlertConfigDto> createAlert(@Valid @RequestBody AlertConfigRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(alertConfigService.createAlert(request));
    }
}
