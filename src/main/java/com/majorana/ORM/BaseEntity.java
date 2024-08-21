package com.majorana.ORM;

import jakarta.persistence.Column;

import java.time.LocalDateTime;

/**
 * A simple based entity that has the basic info about when a database entry was
 * created or modified and by who
 */

public class BaseEntity {

    @AutoPopTimestamp(created = true, updated=false)
    @Column(name="created")
    private static LocalDateTime created;

    @AutoPopTimestamp(created = false, updated=true)
    @Updateable
    @Column(name="updated")
    private static LocalDateTime updated;

    @Column(name="created_userid")
    private int created_userid;

    @Updateable
    @Column(name="updated_userid")
    private int updated_userid;




}

