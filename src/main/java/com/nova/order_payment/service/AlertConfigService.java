package com.nova.order_payment.service;

import com.nova.order_payment.domain.AlertConfig;
import com.nova.order_payment.dto.request.AlertConfigRequest;
import com.nova.order_payment.dto.response.AlertConfigDto;
import com.nova.order_payment.exception.ItemNotFoundException;
import com.nova.order_payment.repository.AlertConfigRepository;
import com.nova.order_payment.repository.ItemRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class AlertConfigService {

    private final AlertConfigRepository alertConfigRepository;
    private final ItemRepository itemRepository;

    @Transactional
    public AlertConfigDto createAlert(AlertConfigRequest alertCreationRequest) {
        log.info("Creating alert: itemId={} type={} threshold={}", alertCreationRequest.getItemId(), alertCreationRequest.getType(), alertCreationRequest.getThreshold());
        var item = itemRepository.findById(alertCreationRequest.getItemId())
                .orElseThrow(() -> new ItemNotFoundException(alertCreationRequest.getItemId()));

        AlertConfig alertConfig = AlertConfig.builder()
                .item(item)
                .threshold(alertCreationRequest.getThreshold())
                .type(alertCreationRequest.getType())
                .build();
        AlertConfig savedAlertConfig = alertConfigRepository.save(alertConfig);
        log.info("Alert saved: alertId={} itemId={} type={} threshold={}", savedAlertConfig.getId(), item.getId(), savedAlertConfig.getType(), savedAlertConfig.getThreshold());

        return AlertConfigDto.builder()
                .alertId(savedAlertConfig.getId())
                .itemId(item.getId())
                .itemName(item.getName())
                .threshold(savedAlertConfig.getThreshold())
                .type(savedAlertConfig.getType())
                .build();
    }
}
