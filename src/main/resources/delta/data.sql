INSERT INTO DEAD_QUEUE_JOB_CONFIG (queue_name, delay_minutes, batch_size, initial_delay_seconds, enabled, description)
VALUES
    ('register.tracking.dead.queue',
     1,
     5000,
     30,
     true,
     'Przetwarzanie wiadomości z tworzenia outgoingu'
    ),
    ('tracking.dead.queue',
     1,
     5000,
     20,
     true,
     'Przetwarzanie wiadomości z trackingu'
    )
    ON DUPLICATE KEY UPDATE
                         delay_minutes = VALUES(delay_minutes),
                         batch_size = VALUES(batch_size),
                         initial_delay_seconds = VALUES(initial_delay_seconds),
                         enabled = VALUES(enabled),
                         description = VALUES(description);