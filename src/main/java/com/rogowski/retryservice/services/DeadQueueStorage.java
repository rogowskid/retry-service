package com.rogowski.retryservice.services;

import com.rogowski.retryservice.entities.MessageDeadHistory;

public interface DeadQueueStorage {

    /**
     * Save a message that should no longer be processed
     * @param entity - MessageDeadHistory entity
     */
    void storeDeadMessage(MessageDeadHistory entity);
}
