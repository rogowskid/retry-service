package com.rogowski.retryservice.entities;

import lombok.*;

import java.time.Instant;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MessageDeadHistory {

    private Long id;
    private Instant createdDate;
    private String exchangeName;
    private String queueName;
    private String routingHeader;
    private String messageId;
    private String payload;

    @SuppressWarnings("unused")
    @Builder
    public MessageDeadHistory(String exchangeName,
                              String queueName,
                              String routingHeader,
                              String messageId,
                              String payload
    ) {
        this.exchangeName = exchangeName;
        this.queueName = queueName;
        this.routingHeader = routingHeader;
        this.messageId = messageId;
        this.payload = payload;
        this.createdDate = Instant.now();
    }
}