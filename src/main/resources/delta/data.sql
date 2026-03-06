INSERT INTO DEAD_QUEUE_JOB_CONFIG (queue_name, delay_minutes, batch_size, initial_delay_seconds, enabled, description,
                                   max_retry)
VALUES
    ('register.tracking.dead.queue',
     1,
     5000,
     30,
     true,
     'Przetwarzanie wiadomości z tworzenia outgoingu',
     5
    ),
    ('tracking.dead.queue',
     1,
     5000,
     20,
     true,
     'Przetwarzanie wiadomości z trackingu',
     3
    ),
    ('logs.dead.queue',
     1,
     50,
     15,
     true,
     'Przetwarzanie wiadomości z niedodanych logow',
     3)
    ON DUPLICATE KEY UPDATE
                         delay_minutes = VALUES(delay_minutes),
                         batch_size = VALUES(batch_size),
                         initial_delay_seconds = VALUES(initial_delay_seconds),
                         enabled = VALUES(enabled),
                         description = VALUES(description),
                         max_retry = VALUES(max_retry);