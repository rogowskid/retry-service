package com.rogowski.retryservice.dtos;

public record MessageResendResponse(
        String queueName,
        int successCount,
        int failureCount
) {
}
