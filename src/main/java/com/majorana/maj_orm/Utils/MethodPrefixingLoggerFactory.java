package com.majorana.Utils;

public class MethodPrefixingLoggerFactory {

    public static MethodPrefixingLogger getLogger(Class target){
        return new MethodPrefixingLogger(target);
    }

}
