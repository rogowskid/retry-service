package com.rogowski.retryservice.services.impl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import com.rogowski.retryservice.dtos.MessageWithDeliveryTag;
import com.rogowski.retryservice.exceptions.AcknowledgeException;
import com.rogowski.retryservice.exceptions.RejectException;
import com.rogowski.retryservice.mapers.MessageDataMapper;
import com.rogowski.retryservice.services.RabbitMQManualAckService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

@Component
@RequiredArgsConstructor
@Slf4j
public class RabbitMQManualAckServiceImpl implements RabbitMQManualAckService {

    private final ConnectionFactory connectionFactory;
    private final MessageDataMapper messageDataMapper;

    @Override
    public List<MessageWithDeliveryTag> fetchMessages(final String queueName, final int batchSize) {
        final List<MessageWithDeliveryTag> messages = new ArrayList<>();
        log.info("Starting fetching messages from queue [{}]...", queueName);

        try (Connection connection = connectionFactory.createConnection();
             Channel channel = connection.createChannel(false)
        ) {
            for (int i = 0; i < batchSize; i++) {
                GetResponse getResponse = channel.basicGet(queueName, false);

                if (getResponse == null) {
                    log.info("Queue [{}] all messages have been retrieved", queueName);
                    break;
                }
                final Message message = messageDataMapper
                        .toSpringMessage(getResponse);
                final long deliveryTag = getResponse.getEnvelope().getDeliveryTag();

                messages.add(new MessageWithDeliveryTag(message, deliveryTag, channel));

                log.debug("Fetched message {} from queue: [{}]", deliveryTag, queueName);
            }
            log.info("Successfully fetched {} messages from queue: [{}]", messages.size(), queueName);
        } catch (IOException | TimeoutException e) {
            log.error("Error while fetching messages from queue: [{}]", queueName, e);
            throw new RuntimeException("Failed to fetch messages from queue: " + queueName);
        }

        return messages;
    }

    @Override
    public void acknowledgeMessage(final String queueName, final Channel channel, final long deliveryTag) {
        try {
            channel.basicAck(deliveryTag, true);
        } catch (Exception e) {
            log.error("Error while acknowledging message: [{}] from queue: [{}]", deliveryTag, queueName, e);
            throw new AcknowledgeException(String.format("Failed to acknowledge message: %s from queue: %s",
                    deliveryTag, queueName));
        }
    }

    @Override
    public void rejectMessage(final String queueName, final Channel channel, final long deliveryTag) {
        try {
            channel.basicNack(deliveryTag, false, true);
        } catch (Exception e) {
            log.error("Error while rejecting message: [{}] from queue: [{}]", deliveryTag, queueName, e);
            throw new RejectException(String.format("Failed to reject message: %s from queue: %s",
                    deliveryTag, queueName));
        }
    }
}
