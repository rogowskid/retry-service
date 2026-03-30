package com.rogowski.retryservice.services;

import com.rabbitmq.client.Channel;
import com.rogowski.retryservice.dtos.MessageWithDeliveryTag;

import java.io.IOException;

public interface RabbitMQManualAckService {

    /**
     * Acknowledge message
     *
     * @param queueName   - queue name
     * @param channel     - channel
     * @param deliveryTag - id a message
     */
    void acknowledgeMessage(String queueName, Channel channel, long deliveryTag);

    /**
     * Fetch message without automatic confirmation (ack)
     *
     * @param channel   - channel
     * @param queueName - queue name
     * @return - message
     */
    MessageWithDeliveryTag getMessage(Channel channel, String queueName) throws IOException;

    long getMessagesCount(Channel channel, String queueName) throws IOException;

    /**
     * Reject message
     *
     * @param queueName   - queue name
     * @param channel     - channel
     * @param deliveryTag - id a message
     */
    void rejectMessage(String queueName, Channel channel, long deliveryTag);
}
