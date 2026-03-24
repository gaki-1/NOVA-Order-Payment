package com.nova.order_payment.service;

import com.nova.order_payment.domain.*;
import com.nova.order_payment.dto.request.CreateOrderRequest;
import com.nova.order_payment.dto.response.OrderDto;
import com.nova.order_payment.exception.InsufficientStockException;
import com.nova.order_payment.exception.InvalidOrderStateException;
import com.nova.order_payment.exception.ItemNotFoundException;
import com.nova.order_payment.exception.OrderNotFoundException;
import com.nova.order_payment.exception.SystemBusyException;
import com.nova.order_payment.kafka.event.OrderEvent;
import com.nova.order_payment.kafka.event.OrderEventType;
import com.nova.order_payment.kafka.producer.InventoryEventProducer;
import com.nova.order_payment.kafka.producer.OrderEventProducer;
import com.nova.order_payment.repository.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock private OrderRepository orderRepository;
    @Mock private OrderItemRepository orderItemRepository;
    @Mock private ItemRepository itemRepository;
    @Mock private InventoryRepository inventoryRepository;
    @Mock private OrderEventProducer orderEventProducer;
    @Mock private InventoryEventProducer inventoryEventProducer;
    @Mock private RedissonClient redissonClient;
    @Mock private RLock mockLock;
    @Mock private TransactionTemplate transactionTemplate;

    @InjectMocks
    private OrderService orderService;

    private UUID itemId;
    private UUID orderId;

    private Item item;
    private Order pendingOrder;
    private Order confirmedOrder;
    private OrderItem orderItem;

    @BeforeEach
    void setUp() {
        itemId = UUID.randomUUID();
        orderId = UUID.randomUUID();

        item = Item.builder()
                .id(itemId)
                .name("Milk")
                .price(BigDecimal.valueOf(2.50))
                .build();

        pendingOrder = Order.builder()
                .id(orderId)
                .status(OrderStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        confirmedOrder = Order.builder()
                .id(orderId)
                .status(OrderStatus.CONFIRMED)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        orderItem = OrderItem.builder()
                .id(UUID.randomUUID())
                .order(pendingOrder)
                .item(item)
                .quantity(5)
                .priceSnapshot(BigDecimal.valueOf(2.50))
                .build();
    }

    // ── createOrder ──────────────────────────────────────────────────────────

    @Test
    void createOrder_createsPendingOrderWithItems() {
        CreateOrderRequest orderCreationRequest = CreateOrderRequest.builder()
                .items(List.of(CreateOrderRequest.OrderLineRequest.builder()
                        .itemId(itemId)
                        .quantity(5)
                        .build()))
                .build();

        when(itemRepository.findById(itemId)).thenReturn(Optional.of(item));
        when(orderRepository.save(any(Order.class))).thenReturn(pendingOrder);
        when(orderItemRepository.save(any(OrderItem.class))).thenReturn(orderItem);

        OrderDto result = orderService.createOrder(orderCreationRequest);

        assertThat(result.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(result.getItems()).hasSize(1);
        assertThat(result.getItems().get(0).getQuantity()).isEqualTo(5);

        ArgumentCaptor<Order> orderCaptor = ArgumentCaptor.forClass(Order.class);
        verify(orderRepository).save(orderCaptor.capture());
        assertThat(orderCaptor.getValue().getStatus()).isEqualTo(OrderStatus.PENDING);
    }

    @Test
    void createOrder_throwsItemNotFoundException_whenItemDoesNotExist() {
        UUID unknownItemId = UUID.randomUUID();
        when(orderRepository.save(any(Order.class))).thenReturn(pendingOrder);
        when(itemRepository.findById(unknownItemId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> orderService.createOrder(CreateOrderRequest.builder()
                .items(List.of(CreateOrderRequest.OrderLineRequest.builder()
                        .itemId(unknownItemId).quantity(1).build()))
                .build()))
                .isInstanceOf(ItemNotFoundException.class)
                .hasMessageContaining(unknownItemId.toString());
    }

    @Test
    void createOrder_capturesPriceSnapshotFromItemAtTimeOfOrder() {
        when(itemRepository.findById(itemId)).thenReturn(Optional.of(item));
        when(orderRepository.save(any(Order.class))).thenReturn(pendingOrder);
        when(orderItemRepository.save(any(OrderItem.class))).thenAnswer(i -> i.getArgument(0));

        orderService.createOrder(CreateOrderRequest.builder()
                .items(List.of(CreateOrderRequest.OrderLineRequest.builder()
                        .itemId(itemId).quantity(3).build()))
                .build());

        ArgumentCaptor<OrderItem> orderItemCaptor = ArgumentCaptor.forClass(OrderItem.class);
        verify(orderItemRepository).save(orderItemCaptor.capture());
        assertThat(orderItemCaptor.getValue().getPriceSnapshot()).isEqualByComparingTo(item.getPrice());
    }

    @Test
    void createOrder_savesAllLineItems_whenMultipleItemsOrdered() {
        UUID secondItemId = UUID.randomUUID();
        Item secondItem = Item.builder().id(secondItemId).name("Bread").price(BigDecimal.valueOf(1.20)).build();
        OrderItem secondOrderItem = OrderItem.builder()
                .id(UUID.randomUUID()).order(pendingOrder).item(secondItem)
                .quantity(2).priceSnapshot(BigDecimal.valueOf(1.20)).build();

        when(orderRepository.save(any(Order.class))).thenReturn(pendingOrder);
        when(itemRepository.findById(itemId)).thenReturn(Optional.of(item));
        when(itemRepository.findById(secondItemId)).thenReturn(Optional.of(secondItem));
        when(orderItemRepository.save(any(OrderItem.class))).thenReturn(orderItem).thenReturn(secondOrderItem);

        OrderDto result = orderService.createOrder(CreateOrderRequest.builder()
                .items(List.of(
                        CreateOrderRequest.OrderLineRequest.builder().itemId(itemId).quantity(5).build(),
                        CreateOrderRequest.OrderLineRequest.builder().itemId(secondItemId).quantity(2).build()))
                .build());

        assertThat(result.getItems()).hasSize(2);
        verify(orderItemRepository, times(2)).save(any(OrderItem.class));
    }

    // ── placeOrder ───────────────────────────────────────────────────────────

    private void setupRedisLockSuccess() throws InterruptedException {
        when(redissonClient.getLock(anyString())).thenReturn(mockLock);
        when(mockLock.tryLock(anyLong(), anyLong(), any())).thenReturn(true);
        when(mockLock.isHeldByCurrentThread()).thenReturn(true);
        when(transactionTemplate.execute(any())).thenAnswer(inv -> {
            TransactionCallback<?> cb = inv.getArgument(0);
            return cb.doInTransaction(mock(TransactionStatus.class));
        });
    }

    @Test
    void placeOrder_confirmsOrder_whenStockIsSufficient() throws Exception {
        setupRedisLockSuccess();
        when(orderItemRepository.findAllByOrder_Id(orderId)).thenReturn(List.of(orderItem));
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(pendingOrder));
        when(inventoryRepository.sumSupplyQuantity(eq(itemId), any())).thenReturn(50.0);
        when(inventoryRepository.sumDemandQuantity(itemId)).thenReturn(0.0);
        when(inventoryRepository.save(any())).thenAnswer(i -> i.getArgument(0));
        when(orderRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        OrderDto result = orderService.placeOrder(orderId);

        assertThat(result.getStatus()).isEqualTo(OrderStatus.CONFIRMED);

        // DEMAND row created
        ArgumentCaptor<Inventory> inventoryCaptor = ArgumentCaptor.forClass(Inventory.class);
        verify(inventoryRepository, atLeastOnce()).save(inventoryCaptor.capture());
        boolean demandCreated = inventoryCaptor.getAllValues().stream()
                .anyMatch(inv -> inv.getType() == InventoryType.DEMAND && inv.getReferenceKey().equals(orderId));
        assertThat(demandCreated).isTrue();

        verify(orderEventProducer).publishOrderEvent(any());
        verify(inventoryEventProducer, atLeastOnce()).publishInventoryEvent(any());
    }

    @Test
    void placeOrder_publishesOrderPlacedEvent_withCorrectTypeAndLines() throws Exception {
        setupRedisLockSuccess();
        when(orderItemRepository.findAllByOrder_Id(orderId)).thenReturn(List.of(orderItem));
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(pendingOrder));
        when(inventoryRepository.sumSupplyQuantity(eq(itemId), any())).thenReturn(50.0);
        when(inventoryRepository.sumDemandQuantity(itemId)).thenReturn(0.0);
        when(inventoryRepository.save(any())).thenAnswer(i -> i.getArgument(0));
        when(orderRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        orderService.placeOrder(orderId);

        ArgumentCaptor<OrderEvent> eventCaptor = ArgumentCaptor.forClass(OrderEvent.class);
        verify(orderEventProducer).publishOrderEvent(eventCaptor.capture());
        assertThat(eventCaptor.getValue().getOrderId()).isEqualTo(orderId);
        assertThat(eventCaptor.getValue().getType()).isEqualTo(OrderEventType.ORDER_PLACED);
        assertThat(eventCaptor.getValue().getOrderLines()).hasSize(1);
        assertThat(eventCaptor.getValue().getOrderLines().get(0).getItemId()).isEqualTo(itemId);
        assertThat(eventCaptor.getValue().getOrderLines().get(0).getQuantity()).isEqualTo(5);
    }

    @Test
    void placeOrder_throwsInsufficientStockException_whenStockIsInsufficient() throws Exception {
        setupRedisLockSuccess();
        when(orderItemRepository.findAllByOrder_Id(orderId)).thenReturn(List.of(orderItem));
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(pendingOrder));
        when(inventoryRepository.sumSupplyQuantity(eq(itemId), any())).thenReturn(3.0); // only 3 available
        when(inventoryRepository.sumDemandQuantity(itemId)).thenReturn(0.0);

        assertThatThrownBy(() -> orderService.placeOrder(orderId))
                .isInstanceOf(InsufficientStockException.class)
                .hasMessageContaining(itemId.toString());

        verify(inventoryRepository, never()).save(any(Inventory.class));
        verify(orderRepository, never()).save(any());
        verify(orderEventProducer, never()).publishOrderEvent(any());
        verify(inventoryEventProducer, never()).publishInventoryEvent(any());
    }

    @Test
    void placeOrder_throwsInsufficientStockException_forSecondItem_whenFirstItemPasses() throws Exception {
        setupRedisLockSuccess();
        UUID secondItemId = UUID.randomUUID();
        Item secondItem = Item.builder().id(secondItemId).name("Bread").price(BigDecimal.valueOf(1.20)).build();
        OrderItem secondOrderItem = OrderItem.builder()
                .id(UUID.randomUUID()).order(pendingOrder).item(secondItem).quantity(10)
                .priceSnapshot(BigDecimal.valueOf(1.20)).build();

        when(orderItemRepository.findAllByOrder_Id(orderId)).thenReturn(List.of(orderItem, secondOrderItem));
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(pendingOrder));

        when(inventoryRepository.sumSupplyQuantity(eq(itemId), any())).thenReturn(50.0);
        when(inventoryRepository.sumDemandQuantity(itemId)).thenReturn(0.0);
        when(inventoryRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        when(inventoryRepository.sumSupplyQuantity(eq(secondItemId), any())).thenReturn(8.0);
        when(inventoryRepository.sumDemandQuantity(secondItemId)).thenReturn(0.0);

        assertThatThrownBy(() -> orderService.placeOrder(orderId))
                .isInstanceOf(InsufficientStockException.class)
                .hasMessageContaining(secondItemId.toString());

        verify(inventoryRepository, times(1)).save(any(Inventory.class));
        verify(orderEventProducer, never()).publishOrderEvent(any());
    }

    @Test
    void placeOrder_throwsException_whenOrderIsNotPending() throws Exception {
        setupRedisLockSuccess();
        when(orderItemRepository.findAllByOrder_Id(orderId)).thenReturn(List.of(orderItem));
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(confirmedOrder));

        assertThatThrownBy(() -> orderService.placeOrder(orderId))
                .isInstanceOf(InvalidOrderStateException.class)
                .hasMessageContaining("PENDING");
    }

    @Test
    void placeOrder_throwsOrderNotFoundException_whenOrderDoesNotExist() throws Exception {
        setupRedisLockSuccess();
        UUID unknownOrderId = UUID.randomUUID();
        when(orderItemRepository.findAllByOrder_Id(unknownOrderId)).thenReturn(List.of(orderItem));
        when(orderRepository.findById(unknownOrderId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> orderService.placeOrder(unknownOrderId))
                .isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    void placeOrder_throwsSystemBusyException_whenLockCannotBeAcquired() throws Exception {
        when(orderItemRepository.findAllByOrder_Id(orderId)).thenReturn(List.of(orderItem));
        when(redissonClient.getLock(anyString())).thenReturn(mockLock);
        when(mockLock.tryLock(anyLong(), anyLong(), any())).thenReturn(false);

        assertThatThrownBy(() -> orderService.placeOrder(orderId))
                .isInstanceOf(SystemBusyException.class)
                .hasMessageContaining(itemId.toString());

        verify(inventoryRepository, never()).save(any());
        verify(orderEventProducer, never()).publishOrderEvent(any());
        verify(inventoryEventProducer, never()).publishInventoryEvent(any());
    }

    @Test
    void placeOrder_releasesLocks_evenWhenExceptionThrown() throws Exception {
        when(orderItemRepository.findAllByOrder_Id(orderId)).thenReturn(List.of(orderItem));
        when(redissonClient.getLock(anyString())).thenReturn(mockLock);
        when(mockLock.tryLock(anyLong(), anyLong(), any())).thenReturn(true);
        when(mockLock.isHeldByCurrentThread()).thenReturn(true);
        when(transactionTemplate.execute(any())).thenAnswer(inv -> {
            TransactionCallback<?> cb = inv.getArgument(0);
            return cb.doInTransaction(mock(TransactionStatus.class));
        });
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(pendingOrder));
        when(inventoryRepository.sumSupplyQuantity(eq(itemId), any())).thenReturn(0.0);
        when(inventoryRepository.sumDemandQuantity(itemId)).thenReturn(0.0);

        assertThatThrownBy(() -> orderService.placeOrder(orderId))
                .isInstanceOf(InsufficientStockException.class);

        verify(mockLock).unlock();
    }

    // ── cancelOrder ──────────────────────────────────────────────────────────

    @Test
    void cancelOrder_throwsOrderNotFoundException_whenOrderDoesNotExist() {
        UUID unknownOrderId = UUID.randomUUID();
        when(orderRepository.findById(unknownOrderId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> orderService.cancelOrder(unknownOrderId))
                .isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    void cancelOrder_restoresStock_andPublishesEvents_whenOrderIsConfirmed() {
        OrderItem confirmedOrderItem = OrderItem.builder()
                .item(item)
                .order(confirmedOrder)
                .quantity(5)
                .priceSnapshot(BigDecimal.valueOf(2.50))
                .build();

        Inventory demandRow = Inventory.builder()
                .item(item)
                .quantity(5.0)
                .type(InventoryType.DEMAND)
                .referenceKey(orderId)
                .build();

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(confirmedOrder));
        when(orderItemRepository.findAllByOrder_Id(orderId)).thenReturn(List.of(confirmedOrderItem));
        when(inventoryRepository.findByItem_IdAndTypeAndReferenceKey(itemId, InventoryType.DEMAND, orderId))
                .thenReturn(Optional.of(demandRow));
        when(orderRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        OrderDto result = orderService.cancelOrder(orderId);

        assertThat(result.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        verify(inventoryRepository).delete(demandRow);

        ArgumentCaptor<OrderEvent> eventCaptor = ArgumentCaptor.forClass(OrderEvent.class);
        verify(orderEventProducer).publishOrderEvent(eventCaptor.capture());
        assertThat(eventCaptor.getValue().getType()).isEqualTo(OrderEventType.ORDER_CANCELLED);

        verify(inventoryEventProducer, atLeastOnce()).publishInventoryEvent(any());
    }

    @Test
    void cancelOrder_doesNotRestoreStock_whenOrderIsPending() {
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(pendingOrder));
        when(orderRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        OrderDto result = orderService.cancelOrder(orderId);

        assertThat(result.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        verify(inventoryRepository, never()).delete(any(Inventory.class));
        verify(orderEventProducer, never()).publishOrderEvent(any());
        verify(inventoryEventProducer, never()).publishInventoryEvent(any());
    }

    @Test
    void cancelOrder_throwsException_whenOrderIsAlreadyCancelled() {
        Order cancelledOrder = Order.builder()
                .id(orderId)
                .status(OrderStatus.CANCELLED)
                .build();

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(cancelledOrder));

        assertThatThrownBy(() -> orderService.cancelOrder(orderId))
                .isInstanceOf(InvalidOrderStateException.class)
                .hasMessageContaining("already cancelled");
    }

    @Test
    void getOrder_returnsOrderDto_whenOrderExists() {
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(confirmedOrder));
        when(orderItemRepository.findAllByOrder_Id(orderId)).thenReturn(List.of(orderItem));

        OrderDto result = orderService.getOrder(orderId);

        assertThat(result.getOrderId()).isEqualTo(orderId);
        assertThat(result.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(result.getItems()).hasSize(1);
        assertThat(result.getTotalPrice()).isEqualByComparingTo(BigDecimal.valueOf(12.50)); // 5 * 2.50
    }

    @Test
    void getOrder_throwsOrderNotFoundException_whenOrderDoesNotExist() {
        UUID unknownOrderId = UUID.randomUUID();
        when(orderRepository.findById(unknownOrderId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> orderService.getOrder(unknownOrderId))
                .isInstanceOf(OrderNotFoundException.class);
    }

    // ── concurrency ───────────────────────────────────────────────────────────

    @Test
    void placeOrder_noOverbooking_under1000ConcurrentRequests() throws InterruptedException {
        int totalStock = 10;   // 10 units available
        int threadCount = 1000;

        // Shared mutable state simulating the DB
        AtomicInteger demandCount = new AtomicInteger(0); // number of DEMAND rows saved

        // Real lock backing the mock — this is what enforces mutual exclusion
        ReentrantLock realLock = new ReentrantLock();

        lenient().when(redissonClient.getLock(anyString())).thenReturn(mockLock);
        lenient().when(mockLock.tryLock(anyLong(), anyLong(), any())).thenAnswer(inv -> {
            long waitTime = inv.getArgument(0);
            TimeUnit unit = inv.getArgument(2);
            return realLock.tryLock(waitTime, unit);
        });
        lenient().when(mockLock.isHeldByCurrentThread()).thenAnswer(inv -> realLock.isHeldByCurrentThread());
        lenient().doAnswer(inv -> { realLock.unlock(); return null; }).when(mockLock).unlock();

        lenient().when(transactionTemplate.execute(any())).thenAnswer(inv -> {
            TransactionCallback<?> cb = inv.getArgument(0);
            return cb.doInTransaction(mock(TransactionStatus.class));
        });

        // Each thread gets a fresh PENDING order so they don't see each other's status change
        lenient().when(orderRepository.findById(any())).thenAnswer(inv ->
                Optional.of(Order.builder()
                        .id(inv.getArgument(0))
                        .status(OrderStatus.PENDING)
                        .createdAt(LocalDateTime.now())
                        .updatedAt(LocalDateTime.now())
                        .build()));
        lenient().when(orderRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        // All threads compete for the same item (quantity=1 per order)
        OrderItem singleUnitOrderItem = OrderItem.builder()
                .id(UUID.randomUUID()).order(pendingOrder).item(item)
                .quantity(1).priceSnapshot(BigDecimal.valueOf(2.50)).build();
        lenient().when(orderItemRepository.findAllByOrder_Id(any())).thenReturn(List.of(singleUnitOrderItem));

        // Inventory state: fixed supply, demand grows as DEMAND rows are saved
        lenient().when(inventoryRepository.sumSupplyQuantity(any(), any())).thenReturn((double) totalStock);
        lenient().when(inventoryRepository.sumDemandQuantity(any())).thenAnswer(inv -> (double) demandCount.get());
        lenient().when(inventoryRepository.save(any(Inventory.class))).thenAnswer(inv -> {
            Inventory saved = inv.getArgument(0);
            if (saved.getType() == InventoryType.DEMAND) {
                demandCount.incrementAndGet();
            }
            return saved;
        });

        ExecutorService executor = Executors.newFixedThreadPool(50);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            UUID threadOrderId = UUID.randomUUID();
            executor.submit(() -> {
                try {
                    orderService.placeOrder(threadOrderId);
                    successCount.incrementAndGet();
                } catch (InsufficientStockException | SystemBusyException e) {
                    failCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        // All threads must have completed (no silent failures)
        assertThat(successCount.get() + failCount.get()).isEqualTo(threadCount);
        // No overbooking: DEMAND rows created must not exceed available supply
        assertThat(demandCount.get()).isLessThanOrEqualTo(totalStock);
        assertThat(successCount.get()).isLessThanOrEqualTo(totalStock);
    }
}
