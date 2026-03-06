package com.rogowski.retryservice.services;

import com.rogowski.retryservice.entities.DeadQueueJobConfigDto;

import java.util.List;

public interface DeadQueueJobConfigService {

    List<DeadQueueJobConfigDto> getAllActiveJobConfigs();

    DeadQueueJobConfigDto getJobConfigById(Long id);

    DeadQueueJobConfigDto createJobConfig(DeadQueueJobConfigDto config);

    DeadQueueJobConfigDto updateJobConfig(Long id, DeadQueueJobConfigDto config);

}
