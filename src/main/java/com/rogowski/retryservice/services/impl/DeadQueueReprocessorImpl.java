package com.rogowski.retryservice.services.impl;

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
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeadQueueReprocessorImpl implements DeadQueueReprocessor {

    private final RabbitTemplate rabbitTemplate;
    private final RabbitMQManualAckService rabbitMQManualAckService;
    private final DeadQueueStorage queueStorage;
    private final MessageDataMapper dataMapper;
    @Value("${dead.max.retries}")
    private Integer maxRetries;

    @Override
    public MessageResendResponse processSingleDeadQueue(final String deadQueueName, final int batchSize) {
        List<MessageWithDeliveryTag> messages = rabbitMQManualAckService.fetchMessages(deadQueueName, batchSize);

        return processMessages(deadQueueName, maxRetries, messages);
    }

    @Override
    public MessageResendResponse processSingleDeadQueue(final String deadQueueName,
                                                        final int batchSize,
                                                        final int maxRetry) {
        List<MessageWithDeliveryTag> messages = rabbitMQManualAckService.fetchMessages(deadQueueName, batchSize);

        return processMessages(deadQueueName, maxRetry, messages);
    }

    private MessageResendResponse processMessages(final String queueName,
                                                  final int maxRetry,
                                                  final List<MessageWithDeliveryTag> messages) {
        int successCount = 0;
        int failureCount = 0;

        List<MessageWithDeliveryTag> remainingMessages = new ArrayList<>(messages);

        try {
            Iterator<MessageWithDeliveryTag> iterator = remainingMessages.iterator();
            while (iterator.hasNext()) {
                MessageWithDeliveryTag message = iterator.next();
                try {
                    resendMessage(message.message(), maxRetry);
                    rabbitMQManualAckService.acknowledgeMessage(queueName, message.channel(), message.deliveryTag());
                    successCount++;
                } catch (Exception ex) {
                    log.error("Error while resending a message from queue: [{}], rejecting.", queueName, ex);
                    rabbitMQManualAckService.rejectMessage(queueName, message.channel(), message.deliveryTag());
                    failureCount++;
                } finally {
                    iterator.remove();
                }
            }
        } catch (Throwable t) {
            log.error("A catastrophic error occurred during message processing for queue [{}]. " +
                    "Rejecting {} remaining messages", queueName, remainingMessages.size(), t);

            for (MessageWithDeliveryTag messageToReject : remainingMessages) {
                try {
                    rabbitMQManualAckService.rejectMessage(queueName,
                            messageToReject.channel(),
                            messageToReject.deliveryTag()
                    );
                    failureCount++;
                } catch (RejectException re) {
                    log.error("Error while rejecting message: [{}] from queue: [{}]",
                            messageToReject.deliveryTag(), queueName, re);
                }
            }
        }

        log.info("Processing completed for queue: {}. Success: {}, Failures: {}",
                queueName, successCount, failureCount);

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
