package com.rogowski.retryservice.services.impl;

import com.rogowski.retryservice.entities.MessageDeadHistory;
import com.rogowski.retryservice.services.DeadQueueStorage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeadQueueStorageImpl implements DeadQueueStorage {

    private final JdbcTemplate jdbcTemplate;
    @Value("${spring.jdbc.insert.sql}")
    private String insertSQL;

    @Override
    @Transactional
    public void storeDeadMessage(MessageDeadHistory entity) {
        if (entity == null) {
            return;
        }

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
