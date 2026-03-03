package com.rogowski.retryservice.exceptions;

public class AcknowledgeException extends RuntimeException {
    public AcknowledgeException(final String message) {
        super(message);
    }
}
