package com.majorana.entities.ex1;

import com.majorana.ORM.*;
import jakarta.persistence.Column;
import jakarta.persistence.Id;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 *  The base entity any Maj DB entity is a super class of
 *  include integer id for sql dbs and a UUID for cassandra
 *  and a created, modified and deleted dates to provide
 *  a audit trail of usage of the data
 *
 */

public abstract class BaseMajoranaEntity {


//    @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0, name = "id")
    @Id()
    @Column(name="id")
    protected int id;

//    @org.springframework.data.cassandra.core.mapping.Column("id")
    @Column(name="uuid")
    @Nullable
    @Id()
    protected UUID uuid;
    @Updateable
//    @org.springframework.data.cassandra.core.mapping.Column("deleted")
    @Column(name="deleted")
    protected boolean deleted;
    @Updateable
    @Nullable
//    @org.s pringframework.data.cassandra.core.mapping.Column("deletedAt")
    @Column(name="deleted_at")
    protected LocalDateTime deletedAt;
//    @org.springframework.data.cassandra.core.mapping.Column("created_by_userid")
    @Column(name="created_by_userid")
    protected int createdByUserid;
//    @org.springframework.data.cassandra.core.mapping.Column("update_by_userid")
    @Column(name="updated_by_userid")
    protected int updatedByUserid;
    @PopulatedCreated
    @AutoPopTimestamp(updated = false , created = true)
//    @org.springframework.data.cassandra.core.mapping.Column("created")
    @Column(name="created")
    protected LocalDateTime created;
    @Updateable
    @PopulatedUpdated
    @AutoPopTimestamp(updated = false , created = true)
//    @org.springframework.data.cassandra.core.mapping.Column("updated")
    @Column(name="updated")
    protected LocalDateTime updated;
    @Column(name="created_by_useremail")
//    @org.springframework.data.cassandra.core.mapping.Column("created_by_useremail")
    protected transient String createdByUserEmail;
//    @org.springframework.data.cassandra.core.mapping.Column("updated_by_useremail")
    @Column(name="updated_by_useremail")
    protected transient String updatedByUserEmail;

    private static String baseFieldss = "id, uuid, deleted, deleted_at, created, created_at, updated, updated_by_userid, created_by_userid" +
            " updated_by_useremail, " +
            " created_by_useremail, " +
            " created_by_userid ";

    public abstract String getTableName();

    public String getBaseFields(){
        return baseFieldss;
    }

    public abstract String getFields();

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public int getCreatedByUserid() {
        return createdByUserid;
    }

    public void setCreatedByUserid(int createdByUserid) {
        this.createdByUserid = createdByUserid;
    }

    public LocalDateTime getCreated() {
        return created;
    }

    public void setCreated(LocalDateTime created) {
        this.created = created;
    }

    public LocalDateTime getUpdated() {
        return updated;
    }

    public void setUpdated(LocalDateTime updated) {
        this.updated = updated;
    }

    public String getCreatedByUserEmail() {
        return createdByUserEmail;
    }

    public void setCreatedByUserEmail(String createdByUserEmail) {
        this.createdByUserEmail = createdByUserEmail;
    }


    public LocalDateTime getDeletedAt() {
        return deletedAt;
    }

    public void setDeletedAt(LocalDateTime deletedAt) {
        this.deletedAt = deletedAt;
    }

    public int getUpdatedByUserid() {
        return updatedByUserid;
    }

    public void setUpdatedByUserid(int updatedByUserid) {
        this.updatedByUserid = updatedByUserid;
    }

    public String getUpdatedByUserEmail() {
        return updatedByUserEmail;
    }

    public void setUpdatedByUserEmail(String updatedByUserEmail) {
        this.updatedByUserEmail = updatedByUserEmail;
    }

    public String toString() {
        return "DE_BaseCostEntity{" +
                "id=" + id +
                ", deleted=" + deleted +
                ", deletedAt=" + deletedAt +
                ", createdByUserid=" + createdByUserid +
                ", updatedByUserid=" + updatedByUserid +
                ", created=" + created +
                ", updated=" + updated +
                ", createdByUserEmail='" + createdByUserEmail + '\'' +
                ", updatedByUserEmail='" + updatedByUserEmail + '\'' +
                '}';
    }
}
