package com.majorana.Utils;

import java.util.Arrays;

public class MajStringUtls {

    public static boolean isInStringArray( String potentialTargets[], String test){
        return Arrays.stream(potentialTargets).anyMatch(pt -> pt.equals(test));
    }


}
