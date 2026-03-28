package com.rogowski.retryservice.services.impl;

import com.rogowski.retryservice.entities.MessageDeadHistory;
import com.rogowski.retryservice.services.DeadQueueStorage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeadQueueStorageImpl implements DeadQueueStorage {

    private final JdbcTemplate jdbcTemplate;

    @Override
    @Transactional
    public void storeDeadMessage(MessageDeadHistory entity) {
        if (entity == null) {
            return;
        }
        final String insertSQL = "INSERT INTO MESSAGE_DEAD_HISTORY " +
                "  (CREATED_DATE, EXCHANGE_NAME, QUEUE_NAME, ROUTING_HEADER, MESSAGE_ID, PAYLOAD) VALUES (?, ?, ?, ?, ?, ?)";

        jdbcTemplate.update(insertSQL,
                Timestamp.from(entity.getCreatedDate()),
                entity.getExchangeName(),
                entity.getQueueName(),
                entity.getRoutingHeader(),
                entity.getMessageId(),
                entity.getPayload()
        );

        log.info("Stored dead message with id: {} in the database.", entity.getMessageId());
    }
}
