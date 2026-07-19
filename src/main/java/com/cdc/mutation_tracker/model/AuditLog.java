package com.cdc.mutation_tracker.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Entity
@Table(name = "audit_log")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuditLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "table_name")
    private String tableName;

    @Column(name = "row_id")
    private String rowId;

    @Column(name = "operation")
    private String operation;

    @Column(name = "changed_fields")
    private String changedFields;

    @Column(name = "human_readable_log")
    private String humanReadableLog;

    @Column(name = "tags")
    private String tags;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @PrePersist
    public void prePersist() {
        this.createdAt = LocalDateTime.now();
    }
}