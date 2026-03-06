INSERT INTO dead_queue_job_config (queue_name, delay_minutes, batch_size, initial_delay_seconds, enabled, description)
VALUES
    ('register.tracking.dead.queue', 1, 5000, 20, true, 'Przetwarzanie wiadomości z tworzenia outgoingu'),
    ('tracking.dead.queue', 1, 5000, 20, true, 'Przetwarzanie wiadomości z trackingu');