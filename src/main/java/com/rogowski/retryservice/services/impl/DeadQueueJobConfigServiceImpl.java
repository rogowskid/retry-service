package com.rogowski.retryservice.services.impl;

import com.rogowski.retryservice.entities.DeadQueueJobConfigDto;
import com.rogowski.retryservice.exceptions.EntityAlreadyExistsException;
import com.rogowski.retryservice.exceptions.EntityUpdateException;
import com.rogowski.retryservice.exceptions.NotFoundException;
import com.rogowski.retryservice.repositories.DeadQueueJobConfigRepository;
import com.rogowski.retryservice.services.DeadQueueJobConfigService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeadQueueJobConfigServiceImpl implements DeadQueueJobConfigService {

    private final DeadQueueJobConfigRepository repository;

    @Override
    public List<DeadQueueJobConfigDto> getAllActiveJobConfigs() {
        log.debug("Fetching all active job configurations");
        return repository.findAllActive();
    }

    @Override
    public DeadQueueJobConfigDto getJobConfigById(final Long id) {
        return repository.findById(id)
                .orElseThrow(() -> new NotFoundException("Job config not found with id: " + id));
    }

    @Override
    public DeadQueueJobConfigDto createJobConfig(DeadQueueJobConfigDto config) {
        if (repository.existsByQueueName(config.getQueueName()))
            throw new EntityAlreadyExistsException(String.format("Job config for queue: %s already exists",
                    config.getQueueName()));

        log.info("Creating new job config for queue: [{}]", config.getQueueName());
        return repository.create(config);
    }

    @Override
    public DeadQueueJobConfigDto updateJobConfig(Long id, DeadQueueJobConfigDto config) {
        DeadQueueJobConfigDto existing = getJobConfigById(id);

        config.setId(id);
        config.setCreatedAt(existing.getCreatedAt());

        int updated = repository.update(config);
        if (updated == 0) {
            throw new EntityUpdateException("Failed to update job config with id: " + id);
        }

        log.info("Updated job config [ID: {}] for queue: [{}]", id, config.getQueueName());
        return repository.findById(id).orElseThrow();
    }

}
