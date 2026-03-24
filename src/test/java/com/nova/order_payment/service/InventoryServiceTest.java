package com.nova.order_payment.service;

import com.nova.order_payment.domain.Inventory;
import com.nova.order_payment.domain.InventoryType;
import com.nova.order_payment.domain.Item;
import com.nova.order_payment.dto.request.CreateItemRequest;
import com.nova.order_payment.dto.response.InventoryDto;
import com.nova.order_payment.exception.ItemNotFoundException;
import com.nova.order_payment.kafka.producer.InventoryEventProducer;
import com.nova.order_payment.repository.InventoryRepository;
import com.nova.order_payment.repository.ItemRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock
    private ItemRepository itemRepository;

    @Mock
    private InventoryRepository inventoryRepository;

    @Mock
    private InventoryEventProducer inventoryEventProducer;

    @InjectMocks
    private InventoryService inventoryService;

    private UUID itemId;
    private Item item;

    @BeforeEach
    void setUp() {
        itemId = UUID.randomUUID();

        item = Item.builder()
                .id(itemId)
                .name("Milk")
                .description("Fresh milk")
                .price(BigDecimal.valueOf(2.50))
                .build();
    }

    @Test
    void registerItemWithInitialStock_savesItemAndSupplyBatch() {
        CreateItemRequest itemCreationRequest = CreateItemRequest.builder()
                .name("Milk")
                .description("Fresh milk")
                .price(BigDecimal.valueOf(2.50))
                .initialQuantity(100)
                .expiryDate(LocalDate.now().plusDays(10))
                .build();

        when(itemRepository.save(any(Item.class))).thenReturn(item);
        when(inventoryRepository.save(any(Inventory.class))).thenAnswer(i -> i.getArgument(0));

        InventoryDto result = inventoryService.registerItemWithInitialStock(itemCreationRequest);

        assertThat(result.getItemId()).isEqualTo(itemId);
        assertThat(result.getName()).isEqualTo("Milk");
        assertThat(result.getAvailableQuantity()).isEqualTo(100.0);

        ArgumentCaptor<Inventory> inventoryCaptor = ArgumentCaptor.forClass(Inventory.class);
        verify(inventoryRepository).save(inventoryCaptor.capture());
        Inventory savedInventory = inventoryCaptor.getValue();
        assertThat(savedInventory.getType()).isEqualTo(InventoryType.SUPPLY);
        assertThat(savedInventory.getQuantity()).isEqualTo(100.0);
        assertThat(savedInventory.getExpiryDate()).isEqualTo(itemCreationRequest.getExpiryDate());
    }

    @Test
    void getItemInventory_returnsInventoryDto_whenItemExists() {
        LocalDate today = LocalDate.now();
        when(itemRepository.findById(itemId)).thenReturn(Optional.of(item));
        when(inventoryRepository.sumSupplyQuantity(itemId, today)).thenReturn(80.0);
        when(inventoryRepository.sumDemandQuantity(itemId)).thenReturn(20.0);
        when(inventoryRepository.findSupplyBatches(eq(itemId), eq(InventoryType.SUPPLY), eq(today)))
                .thenReturn(List.of());

        InventoryDto result = inventoryService.getItemInventory(itemId);

        assertThat(result.getItemId()).isEqualTo(itemId);
        assertThat(result.getAvailableQuantity()).isEqualTo(60.0); // 80 supply - 20 demand
    }

    @Test
    void getItemInventory_throwsItemNotFoundException_whenItemDoesNotExist() {
        UUID unknownItemId = UUID.randomUUID();
        when(itemRepository.findById(unknownItemId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> inventoryService.getItemInventory(unknownItemId))
                .isInstanceOf(ItemNotFoundException.class)
                .hasMessageContaining(unknownItemId.toString());
    }

    @Test
    void getItemsInventory_returnsListOfInventoryDtos() {
        LocalDate today = LocalDate.now();
        UUID secondItemId = UUID.randomUUID();
        Item secondItem = Item.builder()
                .id(secondItemId)
                .name("Bread")
                .price(BigDecimal.valueOf(1.20))
                .build();

        when(itemRepository.findAllByIdIn(List.of(itemId, secondItemId))).thenReturn(List.of(item, secondItem));
        when(inventoryRepository.sumSupplyQuantity(any(UUID.class), eq(today))).thenReturn(50.0);
        when(inventoryRepository.sumDemandQuantity(any(UUID.class))).thenReturn(10.0);
        when(inventoryRepository.findSupplyBatches(any(UUID.class), eq(InventoryType.SUPPLY), eq(today)))
                .thenReturn(List.of());

        List<InventoryDto> result = inventoryService.getItemsInventory(List.of(itemId, secondItemId));

        assertThat(result).hasSize(2);
        assertThat(result).extracting(InventoryDto::getAvailableQuantity).containsOnly(40.0); // 50-10
    }

    @Test
    void registerItemWithInitialStock_returnsNearestExpiryInResponse() {
        LocalDate expiry = LocalDate.now().plusDays(10);
        CreateItemRequest request = CreateItemRequest.builder()
                .name("Milk").description("Fresh milk").price(BigDecimal.valueOf(2.50))
                .initialQuantity(100).expiryDate(expiry).build();

        when(itemRepository.save(any(Item.class))).thenReturn(item);
        when(inventoryRepository.save(any(Inventory.class))).thenAnswer(i -> i.getArgument(0));

        InventoryDto result = inventoryService.registerItemWithInitialStock(request);

        assertThat(result.getNearestExpiry()).isEqualTo(expiry);
    }

    @Test
    void getItemInventory_returnsNearestExpiry_fromActiveBatches() {
        LocalDate today = LocalDate.now();
        LocalDate earlierExpiry = today.plusDays(5);
        LocalDate laterExpiry = today.plusDays(30);

        Inventory earlierBatch = Inventory.builder().item(item).quantity(20.0)
                .type(InventoryType.SUPPLY).expiryDate(earlierExpiry).build();
        Inventory laterBatch = Inventory.builder().item(item).quantity(30.0)
                .type(InventoryType.SUPPLY).expiryDate(laterExpiry).build();

        when(itemRepository.findById(itemId)).thenReturn(Optional.of(item));
        when(inventoryRepository.sumSupplyQuantity(itemId, today)).thenReturn(50.0);
        when(inventoryRepository.sumDemandQuantity(itemId)).thenReturn(0.0);
        when(inventoryRepository.findSupplyBatches(eq(itemId), eq(InventoryType.SUPPLY), eq(today)))
                .thenReturn(List.of(earlierBatch, laterBatch));

        InventoryDto result = inventoryService.getItemInventory(itemId);

        assertThat(result.getNearestExpiry()).isEqualTo(earlierExpiry);
    }

    @Test
    void getItemsInventory_returnsEmptyList_whenNoItemsFound() {
        UUID unknownId = UUID.randomUUID();
        when(itemRepository.findAllByIdIn(List.of(unknownId))).thenReturn(List.of());

        List<InventoryDto> result = inventoryService.getItemsInventory(List.of(unknownId));

        assertThat(result).isEmpty();
    }

    @Test
    void getItemInventory_availableQuantityIsNeverNegative() {
        LocalDate today = LocalDate.now();
        when(itemRepository.findById(itemId)).thenReturn(Optional.of(item));
        when(inventoryRepository.sumSupplyQuantity(itemId, today)).thenReturn(5.0);
        when(inventoryRepository.sumDemandQuantity(itemId)).thenReturn(20.0); // demand > supply edge case
        when(inventoryRepository.findSupplyBatches(any(), any(), any())).thenReturn(List.of());

        InventoryDto result = inventoryService.getItemInventory(itemId);

        assertThat(result.getAvailableQuantity()).isEqualTo(0.0);
    }
}
