package com.rogowski.retryservice.services;

@SuppressWarnings("unused")
public interface DeadQueueReprocessor {

    void processDeadQueues();

    void processSingleDeadQueue(String deadQueueName, Integer batchSize);
}
