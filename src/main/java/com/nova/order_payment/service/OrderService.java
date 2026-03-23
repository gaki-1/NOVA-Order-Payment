package com.nova.order_payment.service;

import com.nova.order_payment.domain.*;
import com.nova.order_payment.dto.request.CreateOrderRequest;
import com.nova.order_payment.dto.response.OrderDto;
import com.nova.order_payment.exception.InsufficientStockException;
import com.nova.order_payment.exception.InvalidOrderStateException;
import com.nova.order_payment.exception.ItemNotFoundException;
import com.nova.order_payment.exception.OrderNotFoundException;
import com.nova.order_payment.exception.SystemBusyException;
import com.nova.order_payment.kafka.event.InventoryEvent;
import com.nova.order_payment.kafka.event.InventoryEventType;
import com.nova.order_payment.kafka.event.OrderEvent;
import com.nova.order_payment.kafka.event.OrderEventType;
import com.nova.order_payment.kafka.producer.InventoryEventProducer;
import com.nova.order_payment.kafka.producer.OrderEventProducer;
import com.nova.order_payment.repository.InventoryRepository;
import com.nova.order_payment.repository.ItemRepository;
import com.nova.order_payment.repository.OrderItemRepository;
import com.nova.order_payment.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final ItemRepository itemRepository;
    private final InventoryRepository inventoryRepository;
    private final OrderEventProducer orderEventProducer;
    private final InventoryEventProducer inventoryEventProducer;
    private final RedissonClient redissonClient;
    private final TransactionTemplate transactionTemplate;

    private record DemandResult(UUID inventoryId, UUID itemId, double quantity) {}
    private record PlaceOrderResult(OrderDto dto, List<DemandResult> demandResults) {}

    @Transactional
    public OrderDto createOrder(CreateOrderRequest orderCreationRequest) {
        log.info("Creating order with {} item(s)", orderCreationRequest.getItems().size());

        Order order = orderRepository.save(Order.builder()
                .status(OrderStatus.PENDING)
                .build());
        log.info("Order created: orderId={} status={}", order.getId(), order.getStatus());

        List<OrderItem> orderItems = orderCreationRequest.getItems().stream()
                .map(orderLineRequest -> buildAndSaveOrderItem(order, orderLineRequest))
                .toList();

        log.info("Order {} has {} line item(s) saved", order.getId(), orderItems.size());
        return buildOrderDto(order, orderItems);
    }

    public OrderDto placeOrder(UUID orderId) {
        log.info("Placing order: orderId={}", orderId);

        List<OrderItem> orderItems = orderItemRepository.findAllByOrder_Id(orderId);

        List<UUID> sortedItemIds = orderItems.stream()
                .map(oi -> oi.getItem().getId())
                .sorted(Comparator.comparing(UUID::toString))
                .distinct()
                .toList();

        List<RLock> locks = sortedItemIds.stream()
                .map(itemId -> redissonClient.getLock("inventory:item:" + itemId))
                .toList();

        try {
            for (int i = 0; i < locks.size(); i++) {
                boolean acquired = locks.get(i).tryLock(2, 10, TimeUnit.SECONDS);
                if (!acquired) {
                    log.warn("Could not acquire Redis lock for itemId={}", sortedItemIds.get(i));
                    throw new SystemBusyException(sortedItemIds.get(i));
                }
                log.debug("Acquired Redis lock for itemId={}", sortedItemIds.get(i));
            }

            PlaceOrderResult txResult = transactionTemplate.execute(
                    status -> executeTransactionalPlaceOrder(orderId));

            // Publish events after transaction commits — no risk of orphaned messages
            orderEventProducer.publishOrderEvent(OrderEvent.builder()
                    .orderId(orderId)
                    .type(OrderEventType.ORDER_PLACED)
                    .orderLines(toOrderLines(orderItems))
                    .build());

            txResult.demandResults().forEach(dr -> inventoryEventProducer.publishInventoryEvent(
                    InventoryEvent.builder()
                            .type(InventoryEventType.DEMAND_CREATED)
                            .inventoryId(dr.inventoryId())
                            .itemId(dr.itemId())
                            .quantity(dr.quantity())
                            .inventoryType(InventoryType.DEMAND)
                            .referenceKey(orderId)
                            .build()));

            return txResult.dto();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while acquiring Redis lock for orderId={}", orderId);
            throw new SystemBusyException(sortedItemIds.isEmpty() ? orderId : sortedItemIds.get(0));
        } finally {
            locks.forEach(lock -> {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                    log.debug("Released Redis lock: {}", lock.getName());
                }
            });
        }
    }

    private PlaceOrderResult executeTransactionalPlaceOrder(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        if (order.getStatus() != OrderStatus.PENDING) {
            throw new InvalidOrderStateException(
                    "Order " + orderId + " is not in PENDING state. Current: " + order.getStatus());
        }

        List<OrderItem> orderItems = orderItemRepository.findAllByOrder_Id(orderId);
        LocalDate today = LocalDate.now();

        log.debug("Placing order {} with {} item(s) — validating stock under Redis lock", orderId, orderItems.size());
        List<DemandResult> demandResults = orderItems.stream()
                .map(oi -> deductStockAndCreateDemand(oi, orderId, today))
                .toList();

        order.setStatus(OrderStatus.CONFIRMED);
        order.setUpdatedAt(LocalDateTime.now());
        orderRepository.save(order);
        log.info("Order {} CONFIRMED", orderId);

        return new PlaceOrderResult(buildOrderDto(order, orderItems), demandResults);
    }

    @Transactional
    public OrderDto cancelOrder(UUID orderId) {
        log.info("Cancelling order: orderId={}", orderId);

        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));

        if (order.getStatus() == OrderStatus.CANCELLED) {
            throw new InvalidOrderStateException("Order " + orderId + " is already cancelled.");
        }

        if (order.getStatus() == OrderStatus.CONFIRMED) {
            log.debug("Order {} was CONFIRMED — restoring stock", orderId);
            List<OrderItem> orderItems = orderItemRepository.findAllByOrder_Id(orderId);
            orderItems.forEach(oi -> restoreStockForOrderItem(oi, orderId));

            orderEventProducer.publishOrderEvent(OrderEvent.builder()
                    .orderId(orderId)
                    .type(OrderEventType.ORDER_CANCELLED)
                    .orderLines(toOrderLines(orderItems))
                    .build());
        }

        order.setStatus(OrderStatus.CANCELLED);
        order.setUpdatedAt(LocalDateTime.now());
        orderRepository.save(order);
        log.info("Order {} CANCELLED", orderId);

        return buildOrderDto(order, orderItemRepository.findAllByOrder_Id(orderId));
    }

    @Transactional(readOnly = true)
    public OrderDto getOrder(UUID orderId) {
        log.debug("Fetching order: orderId={}", orderId);
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
        return buildOrderDto(order, orderItemRepository.findAllByOrder_Id(orderId));
    }

    private OrderItem buildAndSaveOrderItem(Order order, CreateOrderRequest.OrderLineRequest orderLineRequest) {
        Item item = itemRepository.findById(orderLineRequest.getItemId())
                .orElseThrow(() -> new ItemNotFoundException(orderLineRequest.getItemId()));
        log.debug("Adding item to order {}: itemId={} qty={} price={}", order.getId(), item.getId(), orderLineRequest.getQuantity(), item.getPrice());
        return orderItemRepository.save(OrderItem.builder()
                .order(order)
                .item(item)
                .quantity(orderLineRequest.getQuantity())
                .priceSnapshot(item.getPrice())
                .build());
    }

    private DemandResult deductStockAndCreateDemand(OrderItem orderItem, UUID orderId, LocalDate today) {
        double supply = inventoryRepository.sumSupplyQuantity(orderItem.getItem().getId(), today);
        double demand = inventoryRepository.sumDemandQuantity(orderItem.getItem().getId());
        double available = supply - demand;
        log.debug("Stock check (Redis-locked): itemId={} supply={} demand={} available={} requested={}",
                orderItem.getItem().getId(), supply, demand, available, orderItem.getQuantity());

        if (available < orderItem.getQuantity()) {
            log.warn("Insufficient stock: itemId={} requested={} available={}", orderItem.getItem().getId(), orderItem.getQuantity(), available);
            throw new InsufficientStockException(orderItem.getItem().getId(), orderItem.getQuantity(), available);
        }

        Inventory saved = inventoryRepository.save(Inventory.builder()
                .item(orderItem.getItem())
                .quantity((double) orderItem.getQuantity())
                .type(InventoryType.DEMAND)
                .referenceKey(orderId)
                .build());
        log.debug("DEMAND row created: itemId={} qty={} orderId={}", orderItem.getItem().getId(), orderItem.getQuantity(), orderId);

        return new DemandResult(saved.getId(), orderItem.getItem().getId(), saved.getQuantity());
    }

    private void restoreStockForOrderItem(OrderItem orderItem, UUID orderId) {
        inventoryRepository.findByItem_IdAndTypeAndReferenceKey(
                orderItem.getItem().getId(), InventoryType.DEMAND, orderId)
                .ifPresent(demandRow -> {
                    UUID inventoryId = demandRow.getId();
                    Double quantity = demandRow.getQuantity();
                    inventoryRepository.delete(demandRow);
                    log.debug("DEMAND row deleted: itemId={} qty={} orderId={}", orderItem.getItem().getId(), quantity, orderId);

                    inventoryEventProducer.publishInventoryEvent(InventoryEvent.builder()
                            .type(InventoryEventType.DEMAND_REMOVED)
                            .inventoryId(inventoryId)
                            .itemId(orderItem.getItem().getId())
                            .quantity(quantity)
                            .inventoryType(InventoryType.DEMAND)
                            .referenceKey(orderId)
                            .build());
                });
    }

    private List<OrderEvent.OrderLineItem> toOrderLines(List<OrderItem> orderItems) {
        return orderItems.stream()
                .map(oi -> OrderEvent.OrderLineItem.builder()
                        .itemId(oi.getItem().getId())
                        .quantity(oi.getQuantity())
                        .build())
                .toList();
    }

    private OrderDto buildOrderDto(Order order, List<OrderItem> orderItems) {
        List<OrderDto.OrderItemDto> orderItemDtos = orderItems.stream()
                .map(orderItem -> OrderDto.OrderItemDto.builder()
                        .itemId(orderItem.getItem().getId())
                        .itemName(orderItem.getItem().getName())
                        .quantity(orderItem.getQuantity())
                        .priceSnapshot(orderItem.getPriceSnapshot())
                        .build())
                .toList();

        BigDecimal total = orderItemDtos.stream()
                .map(orderItemDto -> orderItemDto.getPriceSnapshot().multiply(BigDecimal.valueOf(orderItemDto.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        return OrderDto.builder()
                .orderId(order.getId())
                .status(order.getStatus())
                .items(orderItemDtos)
                .totalPrice(total)
                .createdAt(order.getCreatedAt())
                .build();
    }
}
