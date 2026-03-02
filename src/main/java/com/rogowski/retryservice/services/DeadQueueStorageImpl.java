package com.rogowski.retryservice.services;

import com.rogowski.retryservice.entities.MessageDeadHistory;
import com.rogowski.retryservice.repositories.MessageDeadHistoryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeadQueueStorageImpl implements DeadQueueStorage {

    private final MessageDeadHistoryRepository repository;

    @Override
    @Transactional
    public void storeDeadMessage(MessageDeadHistory entity) {
        repository.save(entity);
    }
}
