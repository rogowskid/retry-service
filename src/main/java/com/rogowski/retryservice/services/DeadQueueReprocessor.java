package com.rogowski.retryservice.services;

import com.rogowski.retryservice.dtos.MessageResendResponse;

@SuppressWarnings("unused")
public interface DeadQueueReprocessor {

    /**
     * Process a single dead queue
     * @param deadQueueName - queue name
     * @param batchSize - Maximum number of messages to fetch
     * @return - Summary resend and failure messages
     */
    MessageResendResponse processSingleDeadQueue(String deadQueueName, int batchSize);

    /**
     * Process a single dead queue
     * @param deadQueueName - queue name
     * @param batchSize - Maximum number of messages to fetch
     * @param maxRetry - Maximum number of retries
     * @return - Summary resend and failure messages
     */
    MessageResendResponse processSingleDeadQueue(String deadQueueName, int batchSize, int maxRetry);
}
