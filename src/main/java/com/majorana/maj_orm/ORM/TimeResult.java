package com.majorana.maj_orm.ORM;

import jakarta.persistence.TemporalType;
//import org.springframework.data.cassandra.core.mapping.Column;

import java.time.LocalDateTime;

public class TimeResult {

    @jakarta.persistence.Temporal(TemporalType.TIMESTAMP)
    @jakarta.persistence.Column(name="dbtime")
//    @Column("dbtime")
    LocalDateTime datetime;

    public LocalDateTime getDatetime() {
        return datetime;
    }

    public void setDatetime(LocalDateTime datetime) {
        this.datetime = datetime;
    }

    @Override
    public String toString() {
        return "TimeResult{" +
                "datetime=" + datetime +
                '}';
    }
}
