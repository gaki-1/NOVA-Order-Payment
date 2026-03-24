package com.nova.order_payment.controller;

import com.nova.order_payment.dto.request.CreateItemRequest;
import com.nova.order_payment.dto.response.InventoryDto;
import com.nova.order_payment.service.InventoryService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/inventory/items")
@RequiredArgsConstructor
public class InventoryController {

    private final InventoryService inventoryService;

    @PostMapping
    public ResponseEntity<InventoryDto> registerItemWithInitialStock(@Valid @RequestBody CreateItemRequest itemCreationRequest) {
        return ResponseEntity.status(HttpStatus.CREATED).body(inventoryService.registerItemWithInitialStock(itemCreationRequest));
    }

    @GetMapping("/{itemId}")
    public ResponseEntity<InventoryDto> getItemInventory(@PathVariable UUID itemId) {
        return ResponseEntity.ok(inventoryService.getItemInventory(itemId));
    }

    @GetMapping
    public ResponseEntity<List<InventoryDto>> getItemsInventory(@RequestParam List<UUID> ids) {
        return ResponseEntity.ok(inventoryService.getItemsInventory(ids));
    }
}
