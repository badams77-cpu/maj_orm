package Distiller.ORM;

import org.springframework.jdbc.core.PreparedStatementCreator;

public interface MajorPreparedStatCreator extends PreparedStatementCreator {

    void setGenKey(boolean bol0);

}
