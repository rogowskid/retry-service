package com.rogowski.retryservice.services;

import com.rogowski.retryservice.entities.MessageDeadHistory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeadQueueReprocessorImpl implements DeadQueueReprocessor {

    private final RabbitTemplate rabbitTemplate;
    private final DeadQueueStorage queueStorage;
    @Value("${dead.queues.list}")
    private final List<String> deadQueues;
    @Value("${dead.max.retries}")
    private Long maxRetries;


    @Scheduled(fixedDelayString = "${dead.queue.interval}")
    @Override
    public void processDeadQueues() {
        log.info("Starting dead queue reprocessing...");

        deadQueues.parallelStream().forEach(dq -> processSingleDeadQueue(dq, null));

        log.info("Finished dead queue reprocessing...");
    }

    @Override
    public void processSingleDeadQueue(final String deadQueueName, final Integer batchSize) {
        final List<Message> messages = getMessage(deadQueueName, batchSize);

        messages.forEach(this::resendMessage);
    }

    @SuppressWarnings("unchecked")
    private void resendMessage(Message message) {

        final MessageProperties props = message.getMessageProperties();
        final List<Map<String, Object>> xDeath =
                (List<Map<String, Object>>) props.getHeaders().get("x-death");

        if (xDeath == null || xDeath.isEmpty()) {
            log.warn("Message has no x-death header, skipping");
            return;
        }

        final Map<String, Object> deathEntry = xDeath.getFirst();

        final String originalExchange = (String) deathEntry.get("exchange");
        final String queue = (String) deathEntry.get("queue");
        final Long retryCount = (Long) deathEntry.get("count");

        if (retryCount != null && retryCount > maxRetries) {
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

        log.info("Resending message to exchange {}", originalExchange);

        rabbitTemplate.send(originalExchange, "", message);
    }

    private List<Message> getMessage(String queueName, Integer batchSize) {
        final List<Message> messages = new ArrayList<>();
        if (batchSize == null || batchSize == 0) {
            while (true) {
                Message message = getMessage(queueName);
                if (message == null)
                    break;
                messages.add(message);
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                Message receive = getMessage(queueName);
                if (receive != null)
                    messages.add(receive);
            }
        }
        return messages;
    }

    private Message getMessage(String queueName) {
        Message receive = rabbitTemplate.receive(queueName, 1000);
        if (receive == null) {
            log.warn("Queue [{}] all messages have been retrieved", queueName);
            return null;
        }
        return receive;
    }
}
