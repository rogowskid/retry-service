package com.rogowski.retryservice.repositories;

import com.rogowski.retryservice.entities.MessageDeadHistory;
import org.springframework.data.jpa.repository.JpaRepository;

public interface MessageDeadHistoryRepository extends JpaRepository<MessageDeadHistory, Long> {
}
