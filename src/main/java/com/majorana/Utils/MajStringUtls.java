package com.majorana.Utils;

import java.util.Arrays;

public class MajStringUtls {

    /**
     * Checks if string is in array of strings
     *
     * @param potentialTargets
     * @param test
     * @return
     */


    public static boolean isInStringArray( String potentialTargets[], String test){
        return Arrays.stream(potentialTargets).anyMatch(pt -> pt.equals(test));
    }


}
