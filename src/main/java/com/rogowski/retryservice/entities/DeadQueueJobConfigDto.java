package com.rogowski.retryservice.entities;

import lombok.*;

import java.time.Instant;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Builder
public class DeadQueueJobConfigDto {

    private Long id;
    private String queueName;
    private Integer delayMinutes;
    private Integer batchSize;
    private Integer initialDelaySeconds;
    private Boolean enabled;
    private String description;
    private Integer maxRetry;
    private Instant createdAt;
    private Instant updatedAt;

}
