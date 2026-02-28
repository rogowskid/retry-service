package com.rogowski.retryservice.entities;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

@Entity
@Table(name = "MESSAGE_DEAD_HISTORY")
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MessageDeadHistory {

    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Id
    private Long id;

    @Column(name = "CREATED_DATE", updatable = false, nullable = false)
    private Instant createdDate;

    @Column(name = "EXCHANGE_NAME", updatable = false, nullable = false)
    private String exchangeName;

    @Column(name = "QUEUE_NAME", updatable = false, nullable = false)
    private String queueName;

    @Column(name = "ROUTING_HEADER", updatable = false, nullable = false)
    private String routingHeader;

    @Column(name = "MESSAGE_ID", updatable = false)
    private String messageId;

    @Column(name = "PAYLOAD", updatable = false, nullable = false)
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
