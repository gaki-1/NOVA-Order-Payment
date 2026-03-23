package com.nova.order_payment.repository;

import com.nova.order_payment.domain.Inventory;
import com.nova.order_payment.domain.InventoryType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface InventoryRepository extends JpaRepository<Inventory, UUID> {

    // FEFO: read-only SUPPLY batches for inventory reporting (no lock)
    @Query("SELECT i FROM Inventory i WHERE i.item.id = :itemKey AND i.type = :type AND i.expiryDate > :today ORDER BY i.expiryDate ASC")
    List<Inventory> findSupplyBatches(@Param("itemKey") UUID itemKey,
                                      @Param("type") InventoryType type,
                                      @Param("today") LocalDate today);

    // Sum of available SUPPLY quantity (non-expired)
    @Query("SELECT COALESCE(SUM(i.quantity), 0) FROM Inventory i WHERE i.item.id = :itemKey AND i.type = 'SUPPLY' AND i.expiryDate > :today")
    Double sumSupplyQuantity(@Param("itemKey") UUID itemKey, @Param("today") LocalDate today);

    // Sum of DEMAND quantity (all confirmed reservations)
    @Query("SELECT COALESCE(SUM(i.quantity), 0) FROM Inventory i WHERE i.item.id = :itemKey AND i.type = 'DEMAND'")
    Double sumDemandQuantity(@Param("itemKey") UUID itemKey);

    // Find DEMAND row for a specific order
    Optional<Inventory> findByItem_IdAndTypeAndReferenceKey(UUID itemKey, InventoryType type, UUID referenceKey);

    // All inventory rows for an item (for reporting)
    List<Inventory> findAllByItem_Id(UUID itemKey);
}
