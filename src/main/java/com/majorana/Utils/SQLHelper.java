package com.majorana.Utils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SQLHelper {

    // LocalDateTime value to use as a blank timestamp, in timestamp fields that should not hold a NULL value, but indicate that the value is not set
    public static final LocalDateTime BLANK_TIMESTAMP = LocalDateTime.of(1970, 1, 1, 0, 0, 1);

    public static String safeSQL(String s){
        return s.replace("'","''").replace("\\","");
    }


    public static String getDelimitedCodesSafeString(Set<String> codes) {
        List<String> quotedCodes = new ArrayList<>();
        for (String code : codes) {
            quotedCodes.add("'" + SQLHelper.safeSQL(code) + "'");
        }
        return String.join(",", quotedCodes);
    }

    /**
     * Returns a CSV string of parameter tokens for each value in items
     * @param items - the items to generate tokens for
     * @param <T> - type of the collection
     * @return - string of ? tokens separated by commas. If items is null, a blank string is returned
     *
     * e.g. list [1,2,3] will return the string "?,?,?"
     */
    public static <T> String getTokensForCollection(Collection<T> items) {
        if (items == null) { return ""; }

        return items.stream().map(x -> "?").collect(Collectors.joining(","));
    }
}
