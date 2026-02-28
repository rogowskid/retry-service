package com.rogowski.retryservice.services;

import com.rogowski.retryservice.entities.MessageDeadHistory;

public interface DeadQueueStorage {

    void storeDeadMessage(MessageDeadHistory entity);
}
