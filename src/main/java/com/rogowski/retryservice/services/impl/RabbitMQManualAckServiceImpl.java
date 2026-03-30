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
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@RequiredArgsConstructor
@Slf4j
public class RabbitMQManualAckServiceImpl implements RabbitMQManualAckService {

    private final MessageDataMapper messageDataMapper;

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
    public MessageWithDeliveryTag getMessage(Channel channel, String queueName) throws IOException {
        GetResponse getResponse = channel.basicGet(queueName, false);

        if (getResponse == null) {
            log.info("Queue [{}] all messages have been retrieved", queueName);
            return null;
        }
        final Message message = messageDataMapper
                .toSpringMessage(getResponse);
        final long deliveryTag = getResponse.getEnvelope().getDeliveryTag();
        return new MessageWithDeliveryTag(message, deliveryTag, channel);
    }

    @Override
    public long getMessagesCount(Channel channel, String queueName) throws IOException {
        return channel.messageCount(queueName);
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
