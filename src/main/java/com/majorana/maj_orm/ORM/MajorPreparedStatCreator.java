package com.majorana.maj_orm.ORM;

import org.springframework.jdbc.core.PreparedStatementCreator;

/**
 * Help hold the generated keys for an insered new database object in spring jdbc
 */

public interface MajorPreparedStatCreator extends PreparedStatementCreator {

    void setGenKey(boolean bol0);

}
