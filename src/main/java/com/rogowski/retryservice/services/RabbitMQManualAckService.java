package com.rogowski.retryservice.services;

import com.rabbitmq.client.Channel;
import com.rogowski.retryservice.dtos.MessageWithDeliveryTag;

import java.util.List;

public interface RabbitMQManualAckService {

    /**
     * Fetching messages without automatic confirmation (ack)
     *
     * @param queueName - queue name
     * @param batchSize - Maximum number of messages to fetch
     * @return - List of messages with delivery tags
     */
    List<MessageWithDeliveryTag> fetchMessages(String queueName, int batchSize);

    /**
     * Acknowledge message
     *
     * @param queueName   - queue name
     * @param channel     - channel
     * @param deliveryTag - id a message
     */
    void acknowledgeMessage(String queueName, Channel channel, long deliveryTag);

    /**
     * Reject message
     *
     * @param queueName   - queue name
     * @param channel     - channel
     * @param deliveryTag - id a message
     */
    void rejectMessage(String queueName, Channel channel, long deliveryTag);
}
