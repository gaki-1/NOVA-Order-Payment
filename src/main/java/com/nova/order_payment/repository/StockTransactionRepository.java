package com.nova.order_payment.repository;

import com.nova.order_payment.domain.StockTransaction;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface StockTransactionRepository extends JpaRepository<StockTransaction, UUID> {
}
