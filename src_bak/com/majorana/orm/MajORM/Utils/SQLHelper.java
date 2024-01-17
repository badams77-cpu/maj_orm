package com.majorana.orm.MajORM.Utils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SQLHelper {

    // LocalDateTime value to use as a blank timestamp, in timestamp fields that should not hold a NULL value, but indicate that the value is not set
    public static final LocalDateTime BLANK_TIMESTAMP = LocalDateTime.of(1970, 1, 1, 0, 0, 1);
}
