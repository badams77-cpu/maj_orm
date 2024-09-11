package com.majorana.maj_orm.ORM_ACCESS;

import java.util.Arrays;
import java.util.stream.Collectors;

public class FieldPrefix {

    public static String pre(String pre, String fields){
        String[] fieldArray = fields.split(",");
        String out = Arrays.stream(fieldArray).map( f-> pre+"."+f.replaceAll("\\s+","")).collect(Collectors.joining(", "));
        return out;
    }

}
