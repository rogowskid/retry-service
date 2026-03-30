package com.rogowski.retryservice.services.impl;

import com.rabbitmq.client.Channel;
import com.rogowski.retryservice.dtos.MessageResendResponse;
import com.rogowski.retryservice.dtos.MessageWithDeliveryTag;
import com.rogowski.retryservice.entities.MessageDeadHistory;
import com.rogowski.retryservice.exceptions.RejectException;
import com.rogowski.retryservice.mapers.MessageDataMapper;
import com.rogowski.retryservice.services.DeadQueueReprocessor;
import com.rogowski.retryservice.services.DeadQueueStorage;
import com.rogowski.retryservice.services.RabbitMQManualAckService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeadQueueReprocessorImpl implements DeadQueueReprocessor {

    private final ConnectionFactory connectionFactory;
    private final RabbitTemplate rabbitTemplate;
    private final RabbitMQManualAckService rabbitMQManualAckService;
    private final DeadQueueStorage queueStorage;
    private final MessageDataMapper dataMapper;
    @Value("${dead.max.retries}")
    private Integer maxRetries;

    @Override
    public MessageResendResponse processSingleDeadQueue(final String deadQueueName, final int batchSize) {
        return processSingleDeadQueue(deadQueueName, batchSize, maxRetries);
    }

    @Override
    public MessageResendResponse processSingleDeadQueue(final String deadQueueName,
                                                        final int batchSize,
                                                        final int maxRetry) {
        int successCount = 0;
        int failureCount = 0;

        try (Connection connection = connectionFactory.createConnection();
             Channel channel = connection.createChannel(false)
        ) {
            long messagesCount = rabbitMQManualAckService.getMessagesCount(channel, deadQueueName);
            long counter = 0L;

            while (counter < messagesCount && counter < batchSize) {
                MessageWithDeliveryTag message = rabbitMQManualAckService.getMessage(channel, deadQueueName);
                MessageResendResponse response = processMessage(deadQueueName, maxRetry, message);
                successCount += response.successCount();
                failureCount += response.failureCount();
                counter++;
            }
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
        return new MessageResendResponse(deadQueueName, successCount, failureCount);
    }

    private MessageResendResponse processMessage(final String queueName,
                                                 final int maxRetry,
                                                 final MessageWithDeliveryTag message) {
        int successCount = 0;
        int failureCount = 0;

        try {
            try {
                resendMessage(message.message(), maxRetry);
                rabbitMQManualAckService.acknowledgeMessage(queueName, message.channel(), message.deliveryTag());
                successCount++;
            } catch (Exception ex) {
                log.error("Error while resending a message from queue: [{}], rejecting.", queueName, ex);
                rabbitMQManualAckService.rejectMessage(queueName, message.channel(), message.deliveryTag());
                failureCount++;
            }

        } catch (Throwable t) {
            log.error("A catastrophic error occurred during message processing for queue [{}]", queueName, t);

            try {
                rabbitMQManualAckService.rejectMessage(queueName,
                        message.channel(),
                        message.deliveryTag()
                );
                failureCount++;
            } catch (RejectException re) {
                log.error("Error while rejecting message: [{}] from queue: [{}]",
                        message.deliveryTag(), queueName, re);
            }
        }

        log.info("Processing completed message for queue: {}. Success: {}, Failures: {}", queueName,
                successCount, failureCount);

        return new MessageResendResponse(queueName, successCount, failureCount);
    }

    @SuppressWarnings("unchecked")
    private void resendMessage(final Message message, final int maxRetry) {
        final MessageProperties props = message.getMessageProperties();
        final List<Map<String, Object>> xDeath =
                (List<Map<String, Object>>) props.getHeaders().get("x-death");

        if (xDeath == null || xDeath.isEmpty()) {
            log.warn("Message has no x-death header, skipping");
            return;
        }

        final Map<String, Object> deathEntry = xDeath.getFirst();

        final String originalExchange = dataMapper.covertValueToString(deathEntry.get("exchange"));
        final String queue = dataMapper.covertValueToString(deathEntry.get("queue"));
        final Long retryCount = (Long) deathEntry.get("count");

        if (retryCount != null && retryCount > maxRetry) {
            log.error("Message has more than 3 retries, skipping");
            final MessageDeadHistory messageDeadHistory = MessageDeadHistory.builder()
                    .exchangeName(originalExchange)
                    .queueName(queue)
                    .routingHeader(props.getHeaders().keySet().stream()
                            .filter(k -> !k.startsWith("x-"))
                            .collect(Collectors.joining(",")))
                    .messageId(props.getMessageId())
                    .payload(new String(message.getBody()))
                    .build();

            queueStorage.storeDeadMessage(messageDeadHistory);
            return;
        }

        if (originalExchange == null) {
            log.warn("No original exchange found in x-death");
            return;
        }

        rabbitTemplate.send(originalExchange, "", message);
    }
}
