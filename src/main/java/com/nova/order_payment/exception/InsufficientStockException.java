package com.nova.order_payment.exception;

import java.util.UUID;

public class InsufficientStockException extends RuntimeException {
    public InsufficientStockException(UUID itemId, int requested, double available) {
        super("Insufficient stock for item " + itemId + ": requested=" + requested + " available=" + available);
    }
}
