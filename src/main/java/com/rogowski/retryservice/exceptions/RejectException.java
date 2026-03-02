package com.rogowski.retryservice.exceptions;

public class RejectException extends RuntimeException {
    public RejectException(final String message) {
        super(message);
    }
}
