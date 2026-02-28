package com.rogowski.retryservice.controllers;

import com.rogowski.retryservice.services.DeadQueueReprocessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController("/dead-queue-reprocessor")
@RequiredArgsConstructor
@Slf4j
public class DeadQueueReprocessorController {

    private final DeadQueueReprocessor deadQueueReprocessor;

    @PutMapping("/queue")
    public ResponseEntity<Void> reprocessorQueue(@RequestParam String queueName,
                                                 @RequestParam(required = false) Integer batchSize) {
        deadQueueReprocessor.processSingleDeadQueue(queueName, batchSize);
        return ResponseEntity.ok().build();
    }
}
