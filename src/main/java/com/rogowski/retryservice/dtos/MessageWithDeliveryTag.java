package com.rogowski.retryservice.dtos;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;

public record MessageWithDeliveryTag(
        Message message,
        long deliveryTag,
        Channel channel
) {
}
