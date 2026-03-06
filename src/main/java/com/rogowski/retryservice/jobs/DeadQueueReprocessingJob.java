package com.rogowski.retryservice.jobs;

import com.rogowski.retryservice.configs.QuartzConfiguration;
import com.rogowski.retryservice.dtos.MessageResendResponse;
import com.rogowski.retryservice.services.DeadQueueReprocessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@DisallowConcurrentExecution
public class DeadQueueReprocessingJob implements Job {

    private final DeadQueueReprocessor deadQueueReprocessor;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobDataMap dataMap = context.getJobDetail().getJobDataMap();
        String queueName = dataMap.getString(QuartzConfiguration.QUEUE_NAME_KEY);
        int batchSize = dataMap.getInt(QuartzConfiguration.BATCH_SIZE_KEY);
        Long jobConfigId = dataMap.getLong(QuartzConfiguration.JOB_CONFIG_ID_KEY);
        int maxRetry = dataMap.getInt(QuartzConfiguration.MAX_RETRY_KEY);

        log.info("Starting Quartz job [ID: {}] for queue: [{}] with batch size: [{}] and max retry: [{}]",
                jobConfigId, queueName, batchSize, maxRetry);

        try {
            MessageResendResponse response = deadQueueReprocessor.processSingleDeadQueue(queueName, batchSize,
                    maxRetry);
            log.info("Quartz job [ID: {}] completed successfully for queue: [{}]. Success: [{}], Failures: [{}]",
                    jobConfigId, queueName, response.successCount(), response.failureCount());
        } catch (Exception e) {
            log.error("Error executing Quartz job [ID: {}] for queue: [{}]. Job will retry after configured delay.",
                    jobConfigId, queueName, e);
            throw new JobExecutionException("Error executing Quartz job [ID: %s] for queue: [%s]"
                    .formatted(jobConfigId, queueName));
        }
    }
}
