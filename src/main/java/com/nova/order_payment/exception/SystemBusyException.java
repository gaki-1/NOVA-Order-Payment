package com.nova.order_payment.exception;

import java.util.UUID;

public class SystemBusyException extends RuntimeException {
    public SystemBusyException(UUID itemId) {
        super("System is busy processing item " + itemId + ", please retry shortly");
    }
}
