package Distiller.DBs;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Predicate;

/**
 *  Enum DatabaseVariant
 *
 *  For any set of DBCreds specify the database type
 */

public enum DatabaseVariant {

    NONE(0, ""),
    MYSQL( 1, "mysql"),
    SQL_SERVER(2, "sqlserver"),
    CASSANDRA(3, "cassandra");

    private final int code;

    private final String description;

    DatabaseVariant(int code, String description){
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    /**
     * Retrieve the RoleType value from the given code.
     * @param code - the code to match on
     * @return The relevant RoleType value for code, or null if none found
     */
    public static DatabaseVariant getFromCode(int code) {
        return getFromFilter(a -> a.getCode()==code);
    }

    public static DatabaseVariant getFromDescription(String description) {
        return getFromFilter(a -> a.getDescription().equalsIgnoreCase(description));
    }


    // -----------------------------------------------------------------------------------------------------------------
    // Private methods
    // -----------------------------------------------------------------------------------------------------------------

    private static DatabaseVariant getFromFilter(Predicate<DatabaseVariant> filterFunc) {
        Optional<DatabaseVariant> found = Arrays.stream(DatabaseVariant.values()).filter(a -> filterFunc.test(a)).findFirst();
        return found.orElse(null);
    }


}

