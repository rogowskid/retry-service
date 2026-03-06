package com.rogowski.retryservice.repositories;

import com.rogowski.retryservice.entities.DeadQueueJobConfigDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
@RequiredArgsConstructor
@Slf4j
public class DeadQueueJobConfigRepository {

    private final JdbcTemplate jdbcTemplate;

    private final RowMapper<DeadQueueJobConfigDto> rowMapper = (rs, rowNum) ->
            DeadQueueJobConfigDto.builder()
                    .id(rs.getLong("id"))
                    .queueName(rs.getString("queue_name"))
                    .delayMinutes(rs.getInt("delay_minutes"))
                    .batchSize(rs.getInt("batch_size"))
                    .initialDelaySeconds(rs.getInt("initial_delay_seconds"))
                    .enabled(rs.getBoolean("enabled"))
                    .description(rs.getString("description"))
                    .createdAt(rs.getTimestamp("created_at").toInstant())
                    .updatedAt(rs.getTimestamp("updated_at").toInstant())
                    .build();


    public List<DeadQueueJobConfigDto> findAllActive() {
        String sql = "SELECT * FROM dead_queue_job_config WHERE enabled = true ORDER BY id";
        return jdbcTemplate.query(sql, rowMapper);
    }

    public Optional<DeadQueueJobConfigDto> findById(Long id) {
        String sql = "SELECT * FROM dead_queue_job_config WHERE id = ?";
        List<DeadQueueJobConfigDto> results = jdbcTemplate.query(sql, rowMapper, id);
        return results.isEmpty() ? Optional.empty() : Optional.of(results.getFirst());
    }

    public Optional<DeadQueueJobConfigDto> findByQueueName(String queueName) {
        String sql = "SELECT * FROM dead_queue_job_config WHERE queue_name = ?";
        List<DeadQueueJobConfigDto> results = jdbcTemplate.query(sql, rowMapper, queueName);
        return results.isEmpty() ? Optional.empty() : Optional.of(results.getFirst());
    }

    public boolean existsByQueueName(String queueName) {
        String sql = "SELECT COUNT(*) FROM dead_queue_job_config WHERE queue_name = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, queueName);
        return count != null && count > 0;
    }

    public DeadQueueJobConfigDto create(DeadQueueJobConfigDto config) {
        String sql = "INSERT INTO dead_queue_job_config " +
                "(queue_name, delay_minutes, batch_size, initial_delay_seconds, enabled, description, created_at, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        KeyHolder keyHolder = new GeneratedKeyHolder();
        Instant now = Instant.now();

        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, config.getQueueName());
            ps.setInt(2, config.getDelayMinutes());
            ps.setInt(3, config.getBatchSize());
            ps.setInt(4, config.getInitialDelaySeconds() != null ? config.getInitialDelaySeconds() : 0);
            ps.setBoolean(5, config.getEnabled() != null ? config.getEnabled() : true);
            ps.setString(6, config.getDescription());
            ps.setTimestamp(7, Timestamp.from(now));
            ps.setTimestamp(8, Timestamp.from(now));
            return ps;
        }, keyHolder);

        config.setId(keyHolder.getKey().longValue());
        config.setCreatedAt(now);
        config.setUpdatedAt(now);

        return config;
    }

    public int update(DeadQueueJobConfigDto config) {
        String sql = "UPDATE dead_queue_job_config SET " +
                "queue_name = ?, delay_minutes = ?, batch_size = ?, " +
                "initial_delay_seconds = ?, enabled = ?, description = ?, updated_at = ? " +
                "WHERE id = ?";

        return jdbcTemplate.update(sql,
                config.getQueueName(),
                config.getDelayMinutes(),
                config.getBatchSize(),
                config.getInitialDelaySeconds(),
                config.getEnabled(),
                config.getDescription(),
                Timestamp.from(Instant.now()),
                config.getId()
        );
    }

    public int delete(Long id) {
        String sql = "DELETE FROM dead_queue_job_config WHERE id = ?";
        return jdbcTemplate.update(sql, id);
    }

    public int toggleEnabled(Long id, boolean enabled) {
        String sql = "UPDATE dead_queue_job_config SET enabled = ?, updated_at = ? WHERE id = ?";
        return jdbcTemplate.update(sql, enabled, Timestamp.from(Instant.now()), id);
    }

}
