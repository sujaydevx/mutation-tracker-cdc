package com.cdc.mutation_tracker.audit;

import com.cdc.mutation_tracker.model.AuditLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AuditLogRepository extends JpaRepository<AuditLog, Long> {
    List<AuditLog> findByTableNameOrderByCreatedAtDesc(String tableName);
    List<AuditLog> findByTableNameAndRowIdOrderByCreatedAtDesc(
            String tableName, String rowId);
}