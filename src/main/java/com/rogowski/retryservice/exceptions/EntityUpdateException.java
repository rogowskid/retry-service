package com.rogowski.retryservice.exceptions;

public class EntityUpdateException extends RuntimeException {
    public EntityUpdateException(final String message) {
        super(message);
    }
}
