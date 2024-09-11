package com.majorana.ORM_ACCESS;

import com.majorana.ORM.BaseMajoranaEntity;

import java.util.Arrays;
import java.util.stream.Collectors;

public class FieldPrefix {

    public static String pre(String pre, String fields){
        String[] fieldArray = fields.split(",");
        String out = Arrays.stream(fieldArray).map( f-> pre+"."+f.replaceAll("\\s+","")).collect(Collectors.joining(", "));
        return out;
    }

}
