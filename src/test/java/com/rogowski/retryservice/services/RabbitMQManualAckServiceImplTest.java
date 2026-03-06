package com.rogowski.retryservice.services;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.impl.ChannelN;
import com.rogowski.retryservice.dtos.MessageWithDeliveryTag;
import com.rogowski.retryservice.exceptions.AcknowledgeException;
import com.rogowski.retryservice.exceptions.RejectException;
import com.rogowski.retryservice.mapers.MessageDataMapper;
import com.rogowski.retryservice.services.impl.RabbitMQManualAckServiceImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.SimpleConnection;

import java.io.IOException;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class RabbitMQManualAckServiceImplTest {

    @InjectMocks
    private RabbitMQManualAckServiceImpl service;
    @Mock
    private ChannelN channelN;
    @Mock
    private CachingConnectionFactory connection;
    @Mock
    private SimpleConnection simpleConnection;
    @Mock
    private MessageDataMapper messageDataMapper;

    @Test
    void fetchMessages_whenQueueHasMessages_thenGet() throws IOException {
        //given
        Mockito.when(connection.createConnection())
                        .thenReturn(simpleConnection);
        Mockito.when(simpleConnection.createChannel(Mockito.anyBoolean()))
                .thenReturn(channelN);
        Mockito.when(channelN.basicGet(Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn(new GetResponse(new Envelope(1L, false,
                        "test.exchange", ""), new AMQP.BasicProperties(),
                        "test.message".getBytes(), 0));
        Mockito.when(messageDataMapper.toSpringMessage(Mockito.any()))
                .thenReturn(new Message("abc".getBytes()));

        //given
        List<MessageWithDeliveryTag> result = service.fetchMessages("queueName", 10);

        //given
        Assertions.assertEquals(10, result.size());
        Mockito.verify(channelN, Mockito.times(10))
                .basicGet(Mockito.anyString(), Mockito.anyBoolean());
    }

    @Test
    void acknowledgeMessage_whenNoThrowException_thenAcknowledgeMessage() throws IOException {
        //given
        Mockito.doNothing().when(channelN).basicAck(Mockito.anyLong(), Mockito.anyBoolean());

        //when
        service.acknowledgeMessage("queueName", channelN, 1L);

        //then
        Mockito.verify(channelN, Mockito.times(1)).basicAck(Mockito.anyLong(),
                Mockito.anyBoolean());
    }

    @Test
    void acknowledgeMessage_whenThrowException_thenThrowException() throws IOException {
        //given
        Mockito.doThrow(IOException.class).when(channelN).basicAck(Mockito.anyLong(), Mockito.anyBoolean());

        //when
        AcknowledgeException acknowledgeException = Assertions.assertThrowsExactly(AcknowledgeException.class,
                () -> service.acknowledgeMessage("queueName", channelN, 1L));

        Assertions.assertEquals("Failed to acknowledge message: 1 from queue: queueName",
                acknowledgeException.getMessage());
    }

    @Test
    void rejectMessage_whenNoThrowException_thenRejectMessage() throws IOException {
        //given
        Mockito.doNothing().when(channelN).basicNack(Mockito.anyLong(), Mockito.anyBoolean(), Mockito.anyBoolean());

        //when
        service.rejectMessage("queueName", channelN, 1L);

        //then
        Mockito.verify(channelN, Mockito.times(1))
                .basicNack(Mockito.anyLong(), Mockito.anyBoolean(), Mockito.anyBoolean());
    }

    @Test
    void rejectMessage_whenThrowException_thenThrowException() throws IOException {
        //given
        Mockito.doThrow(IOException.class).when(channelN).basicNack(Mockito.anyLong(),
                Mockito.anyBoolean(), Mockito.anyBoolean());

        //when
        RejectException rejectException = Assertions.assertThrowsExactly(RejectException.class,
                () -> service.rejectMessage("queueName", channelN, 1L));

        Assertions.assertEquals("Failed to reject message: 1 from queue: queueName",
                rejectException.getMessage());
    }
}