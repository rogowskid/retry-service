package com.rogowski.retryservice.services.impl;

import com.rabbitmq.client.impl.ChannelN;
import com.rogowski.retryservice.dtos.MessageResendResponse;
import com.rogowski.retryservice.dtos.MessageWithDeliveryTag;
import com.rogowski.retryservice.mapers.MessageDataMapper;
import com.rogowski.retryservice.services.DeadQueueStorage;
import com.rogowski.retryservice.services.RabbitMQManualAckService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class DeadQueueReprocessorImplTest {

    @InjectMocks
    private DeadQueueReprocessorImpl service;
    @Mock
    private RabbitTemplate rabbitTemplate;
    @Mock
    private RabbitMQManualAckService rabbitMQManualAckService;
    @Mock
    private DeadQueueStorage queueStorage;
    @Mock
    private MessageDataMapper dataMapper;
    @Mock
    private ChannelN channelN;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "maxRetries", 3L);
    }

    @Test
    void processSingleDeadQueue_whenMessagesExists_thenReprocessing() {
        //when
        final HashMap<String, Object> xDeathProperties = new HashMap<>();
        xDeathProperties.put("exchange", "exchange.test");
        xDeathProperties.put("queue", "test.queue");
        xDeathProperties.put("count", 1L);
        final HashMap<String, Object> header = new HashMap<>();
        header.put("x-death", List.of(xDeathProperties));
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setHeaders(header);
        List<MessageWithDeliveryTag> messages = List.of(new MessageWithDeliveryTag(
                new Message("abc".getBytes(),
                        messageProperties),
                1L, channelN)
        );
        Mockito.when(rabbitMQManualAckService.fetchMessages(Mockito.anyString(), Mockito.anyInt()))
                .thenReturn(messages);
        Mockito.doNothing().when(rabbitMQManualAckService)
                .acknowledgeMessage(Mockito.anyString(), Mockito.any(ChannelN.class), Mockito.anyLong());
        Mockito.when(dataMapper.covertValueToString("exchange.test"))
                .thenReturn("exchange.test");
        Mockito.when(dataMapper.covertValueToString("test.queue"))
                .thenReturn("test.queue");
        Mockito.doNothing().when(rabbitTemplate).send(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.any(Message.class)
        );

        //when
        MessageResendResponse response = service.processSingleDeadQueue("test.queue", 1);

        //then
        Assertions.assertNotNull(response);
        Assertions.assertEquals(1, response.successCount());
        Assertions.assertEquals(0, response.failureCount());
        Assertions.assertEquals("test.queue", response.queueName());
        Mockito.verify(rabbitMQManualAckService, Mockito.times(1))
                .fetchMessages(Mockito.anyString(), Mockito.anyInt());
        Mockito.verify(rabbitMQManualAckService, Mockito.times(1))
                .acknowledgeMessage(Mockito.anyString(), Mockito.any(ChannelN.class), Mockito.anyLong());
        Mockito.verify(dataMapper, Mockito.times(2))
                .covertValueToString(Mockito.any());
        Mockito.verify(rabbitTemplate, Mockito.times(1))
                .send(Mockito.anyString(), Mockito.anyString(), Mockito.any(Message.class));
    }

    @Test
    void processSingleDeadQueue_whenMessageExistsBhtThrowException_thenRejectMessage(){
        //when
        final HashMap<String, Object> xDeathProperties = new HashMap<>();
        xDeathProperties.put("exchange", "exchange.test");
        xDeathProperties.put("queue", "test.queue");
        xDeathProperties.put("count", 1L);
        final HashMap<String, Object> header = new HashMap<>();
        header.put("x-death", List.of(xDeathProperties));
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setHeaders(header);
        List<MessageWithDeliveryTag> messages = List.of(new MessageWithDeliveryTag(
                new Message("abc".getBytes(),
                        messageProperties),
                1L, channelN)
        );
        Mockito.when(rabbitMQManualAckService.fetchMessages(Mockito.anyString(), Mockito.anyInt()))
                .thenReturn(messages);
        Mockito.doThrow(NullPointerException.class).when(dataMapper)
                .covertValueToString(Mockito.any());


        //when
        MessageResendResponse response = service.processSingleDeadQueue("test.queue", 1);

        //then
        Assertions.assertNotNull(response);
        Assertions.assertEquals(0, response.successCount());
        Assertions.assertEquals(1, response.failureCount());
        Assertions.assertEquals("test.queue", response.queueName());
        Mockito.verify(rabbitMQManualAckService, Mockito.times(1))
                .fetchMessages(Mockito.anyString(), Mockito.anyInt());
        Mockito.verify(rabbitMQManualAckService, Mockito.times(1))
                .rejectMessage(Mockito.anyString(), Mockito.any(ChannelN.class), Mockito.anyLong());
    }
}