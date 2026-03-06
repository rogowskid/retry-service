package com.rogowski.retryservice.services.impl;

import com.rogowski.retryservice.entities.MessageDeadHistory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Timestamp;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

@ExtendWith(MockitoExtension.class)
class DeadQueueStorageImplTest {

    @InjectMocks
    private DeadQueueStorageImpl service;
    @Mock
    private JdbcTemplate jdbcTemplate;


    @Test
    void storeDeadMessage_whenEntityIsNotNull_thenSave() {
        //given
        ReflectionTestUtils.setField(service, "insertSQL", "ABC");
        MessageDeadHistory entity = MessageDeadHistory.builder()
                .exchangeName("exchange.test")
                .queueName("test.queue")
                .routingHeader("test")
                .payload("{'test': '123'}")
                .build();

        Mockito.when(jdbcTemplate.update(
                anyString(),
                any(Timestamp.class),
                anyString(),
                anyString(),
                anyString(),
                any(),
                anyString()
        )).thenReturn(1);

        //when
        service.storeDeadMessage(entity);

        //then
        Mockito.verify(jdbcTemplate, Mockito.times(1)).update(
                anyString(),
                any(Timestamp.class),
                anyString(),
                anyString(),
                anyString(),
                any(),
                anyString()
        );
    }

    @Test
    void storeDeadMessage_whenEntityIsNull_thenDoNotSave() {
        //given
        MessageDeadHistory entity = null;

        //when
        service.storeDeadMessage(entity);

        //then
        Mockito.verifyNoInteractions(jdbcTemplate);
    }
}