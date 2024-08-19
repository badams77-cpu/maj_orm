package com.majorana.ORM;

import java.util.Arrays;

public class MajStringUtls {

    /**
     * Tests weather a target string is in an array
     * 
     * @param potentialTargets
     * @param test
     * @return true if test in is the target srting array
     */
    
    
    public static boolean isInStringArray( String potentialTargets[], String test){
        return Arrays.stream(potentialTargets).anyMatch(pt -> pt.equals(test));
    }


}
