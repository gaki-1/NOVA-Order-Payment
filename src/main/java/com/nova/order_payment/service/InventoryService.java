package com.nova.order_payment.service;

import com.nova.order_payment.domain.Inventory;
import com.nova.order_payment.domain.InventoryType;
import com.nova.order_payment.domain.Item;
import com.nova.order_payment.dto.request.CreateItemRequest;
import com.nova.order_payment.dto.response.InventoryDto;
import com.nova.order_payment.exception.ItemNotFoundException;
import com.nova.order_payment.kafka.event.InventoryEvent;
import com.nova.order_payment.kafka.event.InventoryEventType;
import com.nova.order_payment.kafka.producer.InventoryEventProducer;
import com.nova.order_payment.repository.InventoryRepository;
import com.nova.order_payment.repository.ItemRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryService {

    private final ItemRepository itemRepository;
    private final InventoryRepository inventoryRepository;
    private final InventoryEventProducer inventoryEventProducer;

    @Transactional
    public InventoryDto registerItemWithInitialStock(CreateItemRequest itemCreationRequest) {
        log.info("Registering item: name={} price={} initialQty={}", itemCreationRequest.getName(), itemCreationRequest.getPrice(), itemCreationRequest.getInitialQuantity());
        Item item = Item.builder()
                .name(itemCreationRequest.getName())
                .description(itemCreationRequest.getDescription())
                .price(itemCreationRequest.getPrice())
                .build();
        item = itemRepository.save(item);
        log.info("Item saved: itemId={} name={}", item.getId(), item.getName());

        Inventory supplyInventory = Inventory.builder()
                .item(item)
                .quantity(itemCreationRequest.getInitialQuantity().doubleValue())
                .type(InventoryType.SUPPLY)
                .expiryDate(itemCreationRequest.getExpiryDate())
                .build();
        Inventory savedInventory = inventoryRepository.save(supplyInventory);
        log.info("SUPPLY batch saved: itemId={} qty={} expiry={}", item.getId(), savedInventory.getQuantity(), savedInventory.getExpiryDate());

        inventoryEventProducer.publishInventoryEvent(InventoryEvent.builder()
                .type(InventoryEventType.SUPPLY_ADDED)
                .inventoryId(savedInventory.getId())
                .itemId(item.getId())
                .quantity(savedInventory.getQuantity())
                .inventoryType(InventoryType.SUPPLY)
                .expiryDate(savedInventory.getExpiryDate())
                .build());

        return InventoryDto.builder()
                .itemId(item.getId())
                .name(item.getName())
                .price(item.getPrice())
                .availableQuantity(savedInventory.getQuantity())
                .nearestExpiry(savedInventory.getExpiryDate())
                .build();
    }

    @Transactional(readOnly = true)
    public InventoryDto getItemInventory(UUID itemId) {
        log.debug("Fetching inventory for itemId={}", itemId);
        Item item = itemRepository.findById(itemId)
                .orElseThrow(() -> new ItemNotFoundException(itemId));
        return buildInventoryDto(item);
    }

    @Transactional(readOnly = true)
    public List<InventoryDto> getItemsInventory(List<UUID> ids) {
        log.debug("Fetching inventory for {} item(s)", ids.size());
        return itemRepository.findAllByIdIn(ids).stream()
                .map(this::buildInventoryDto)
                .toList();
    }

    private InventoryDto buildInventoryDto(Item item) {
        LocalDate today = LocalDate.now();
        double supply = inventoryRepository.sumSupplyQuantity(item.getId(), today);
        double demand = inventoryRepository.sumDemandQuantity(item.getId());
        double available = Math.max(0, supply - demand);
        log.info("Inventory snapshot: itemId={} name={} supply={} demand={} available={}", item.getId(), item.getName(), supply, demand, available);

        LocalDate nearestExpiry = inventoryRepository
                .findSupplyBatches(item.getId(), InventoryType.SUPPLY, today)
                .stream()
                .map(Inventory::getExpiryDate)
                .min(LocalDate::compareTo)
                .orElse(null);

        return InventoryDto.builder()
                .itemId(item.getId())
                .name(item.getName())
                .price(item.getPrice())
                .availableQuantity(available)
                .nearestExpiry(nearestExpiry)
                .build();
    }
}
