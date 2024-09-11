package com.majorana.maj_orm.Utils;

public class MethodPrefixingLoggerFactory {

    public static MethodPrefixingLogger getLogger(Class target){
        return new MethodPrefixingLogger(target);
    }

}
