package com.nova.order_payment.service;

import com.nova.order_payment.domain.AlertConfig;
import com.nova.order_payment.domain.AlertType;
import com.nova.order_payment.domain.Item;
import com.nova.order_payment.dto.request.AlertConfigRequest;
import com.nova.order_payment.dto.response.AlertConfigDto;
import com.nova.order_payment.exception.ItemNotFoundException;
import com.nova.order_payment.repository.AlertConfigRepository;
import com.nova.order_payment.repository.ItemRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AlertConfigServiceTest {

    @Mock
    private AlertConfigRepository alertConfigRepository;

    @Mock
    private ItemRepository itemRepository;

    @InjectMocks
    private AlertConfigService alertConfigService;

    private UUID itemId;
    private UUID alertId;
    private Item item;

    @BeforeEach
    void setUp() {
        itemId = UUID.randomUUID();
        alertId = UUID.randomUUID();

        item = Item.builder()
                .id(itemId)
                .name("Milk")
                .price(BigDecimal.valueOf(2.50))
                .build();
    }

    @Test
    void createAlert_savesConfigAndReturnsDto() {
        AlertConfigRequest alertCreationRequest = AlertConfigRequest.builder()
                .itemId(itemId)
                .threshold(10)
                .type(AlertType.LOW_STOCK)
                .build();

        AlertConfig savedAlertConfig = AlertConfig.builder()
                .id(alertId)
                .item(item)
                .threshold(10)
                .type(AlertType.LOW_STOCK)
                .build();

        when(itemRepository.findById(itemId)).thenReturn(Optional.of(item));
        when(alertConfigRepository.save(any(AlertConfig.class))).thenReturn(savedAlertConfig);

        AlertConfigDto result = alertConfigService.createAlert(alertCreationRequest);

        assertThat(result.getAlertId()).isEqualTo(alertId);
        assertThat(result.getItemId()).isEqualTo(itemId);
        assertThat(result.getItemName()).isEqualTo("Milk");
        assertThat(result.getThreshold()).isEqualTo(10);
        assertThat(result.getType()).isEqualTo(AlertType.LOW_STOCK);

        ArgumentCaptor<AlertConfig> alertConfigCaptor = ArgumentCaptor.forClass(AlertConfig.class);
        verify(alertConfigRepository).save(alertConfigCaptor.capture());
        assertThat(alertConfigCaptor.getValue().getThreshold()).isEqualTo(10);
        assertThat(alertConfigCaptor.getValue().getType()).isEqualTo(AlertType.LOW_STOCK);
    }

    @Test
    void createAlert_allowsMultipleThresholdsForSameItem() {
        UUID lowStockAlertId = UUID.randomUUID();
        UUID criticalStockAlertId = UUID.randomUUID();

        AlertConfig savedLowStockConfig = AlertConfig.builder()
                .id(lowStockAlertId)
                .item(item)
                .threshold(20)
                .type(AlertType.LOW_STOCK)
                .build();

        AlertConfig savedCriticalStockConfig = AlertConfig.builder()
                .id(criticalStockAlertId)
                .item(item)
                .threshold(5)
                .type(AlertType.CRITICAL_STOCK)
                .build();

        when(itemRepository.findById(itemId)).thenReturn(Optional.of(item));
        when(alertConfigRepository.save(any()))
                .thenReturn(savedLowStockConfig)
                .thenReturn(savedCriticalStockConfig);

        AlertConfigDto lowStockAlert = alertConfigService.createAlert(AlertConfigRequest.builder()
                .itemId(itemId).threshold(20).type(AlertType.LOW_STOCK).build());
        AlertConfigDto criticalStockAlert = alertConfigService.createAlert(AlertConfigRequest.builder()
                .itemId(itemId).threshold(5).type(AlertType.CRITICAL_STOCK).build());

        assertThat(lowStockAlert.getThreshold()).isEqualTo(20);
        assertThat(criticalStockAlert.getThreshold()).isEqualTo(5);
        verify(alertConfigRepository, times(2)).save(any());
    }

    @Test
    void createAlert_savesConfigLinkedToCorrectItem() {
        when(itemRepository.findById(itemId)).thenReturn(Optional.of(item));
        when(alertConfigRepository.save(any(AlertConfig.class))).thenAnswer(i -> i.getArgument(0));

        alertConfigService.createAlert(AlertConfigRequest.builder()
                .itemId(itemId).threshold(10).type(AlertType.LOW_STOCK).build());

        ArgumentCaptor<AlertConfig> captor = ArgumentCaptor.forClass(AlertConfig.class);
        verify(alertConfigRepository).save(captor.capture());
        assertThat(captor.getValue().getItem().getId()).isEqualTo(itemId);
        assertThat(captor.getValue().getItem().getName()).isEqualTo("Milk");
    }

    @Test
    void createAlert_throwsItemNotFoundException_whenItemDoesNotExist() {
        UUID unknownItemId = UUID.randomUUID();
        when(itemRepository.findById(unknownItemId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> alertConfigService.createAlert(AlertConfigRequest.builder()
                .itemId(unknownItemId).threshold(10).type(AlertType.LOW_STOCK).build()))
                .isInstanceOf(ItemNotFoundException.class)
                .hasMessageContaining(unknownItemId.toString());
    }
}
