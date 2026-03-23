package com.nova.order_payment.repository;

import com.nova.order_payment.domain.AlertConfig;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface AlertConfigRepository extends JpaRepository<AlertConfig, UUID> {

    List<AlertConfig> findAllByItem_Id(UUID itemKey);
}
