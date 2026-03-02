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
    MessageResendResponse processSingleDeadQueue(String deadQueueName, Integer batchSize);
}
