package com.rogowski.retryservice.configs;

import com.rogowski.retryservice.entities.DeadQueueJobConfigDto;
import com.rogowski.retryservice.jobs.DeadQueueReprocessingJob;
import com.rogowski.retryservice.services.DeadQueueJobConfigService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Configuration;

import java.time.Instant;
import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class QuartzConfiguration implements ApplicationRunner {

    private final Scheduler scheduler;
    private final DeadQueueJobConfigService jobConfigService;

    public static final String QUEUE_NAME_KEY = "queueName";
    public static final String BATCH_SIZE_KEY = "batchSize";
    public static final String JOB_CONFIG_ID_KEY = "jobConfigId";
    public static final String MAX_RETRY_KEY = "maxRetry";

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("Loading job configurations from database and scheduling Quartz jobs");
        List<DeadQueueJobConfigDto> activeJobConfigs = jobConfigService.getAllActiveJobConfigs();

        if (activeJobConfigs == null || activeJobConfigs.isEmpty()) {
            log.warn("No active job configuration found in db");
            return;
        }

        for (DeadQueueJobConfigDto config : activeJobConfigs) {
            try {
                scheduleJob(config);
            } catch (Exception e) {
                log.error("Failed to schedule job for queue: [{}]", config.getQueueName(), e);
            }
        }

        log.info("Successfully scheduled {} Quartz jobs from database", activeJobConfigs.size());
    }

    public void scheduleJob(DeadQueueJobConfigDto config) throws SchedulerException {
        String jobName = "DeadQueueJob_" + config.getQueueName().replace(".", "_");
        String triggerName = "DeadQueueTrigger_" + config.getQueueName().replace(".", "_");

        JobDetail jobDetail = JobBuilder.newJob(DeadQueueReprocessingJob.class)
                .withIdentity(jobName, "DeadQueueGroup")
                .withDescription(config.getDescription() != null ? config.getDescription()
                        : "Reprocessing job for queue: " + config.getQueueName())
                .usingJobData(QUEUE_NAME_KEY, config.getQueueName())
                .usingJobData(BATCH_SIZE_KEY, config.getBatchSize())
                .usingJobData(JOB_CONFIG_ID_KEY, config.getId())
                .usingJobData(MAX_RETRY_KEY, config.getMaxRetry())
                .storeDurably()
                .build();

        Instant startTime = Instant.now()
                .plusMillis(config.getInitialDelaySeconds() * 1000L);

        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(triggerName, "DeadQueueGroup")
                .forJob(jobDetail)
                .startAt(startTime)
                .withSchedule(
                        SimpleScheduleBuilder.simpleSchedule()
                                .withIntervalInMinutes(config.getDelayMinutes())
                                .repeatForever()
                                .withMisfireHandlingInstructionFireNow()
                )
                .build();

        if (scheduler.checkExists(jobDetail.getKey())) {
            scheduler.deleteJob(jobDetail.getKey());
            log.info("Removed existing job: [{}]", jobName);
        }

        scheduler.scheduleJob(jobDetail, trigger);
        log.info("Scheduled job [ID: {}]: [{}] for queue: [{}] with delay: [{}] minutes, batch size: [{}]," +
                        " initial delay: [{}] seconds", config.getId(), jobName, config.getQueueName(),
                config.getDelayMinutes(), config.getBatchSize(), config.getInitialDelaySeconds());
    }

}
