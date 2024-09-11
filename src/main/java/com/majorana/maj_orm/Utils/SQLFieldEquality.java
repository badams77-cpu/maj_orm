package com.majorana.maj_orm.Utils;

public class SQLFieldEquality {


    private static final MethodPrefixingLogger LOGGER = MethodPrefixingLoggerFactory.getLogger(RandomEntity.class);

    public static boolean checkField(String f, Object ov, Object ot){
        boolean eq = true;
        if (ov instanceof java.util.Date && ot instanceof java.util.Date){
            if (Math.abs( ((java.util.Date) ot).getTime() - ((java.util.Date) ov).getTime())>60000){
                LOGGER.warn("Date value mismatch "+ov+"!="+ot);
                eq = false;
            }
        } else if (ov instanceof Float && ot instanceof Float){
            double avg =  ((float) ov+ (float) ot)/2.0d;
            if (Math.abs( (float) ov- (float) ot)<0.001 *avg){
                return true;
            }else {
                LOGGER.warn("Float value mismatch "+ov+"!="+ot);
                return false;
            }
        } else if (ot instanceof Double && ot instanceof Double){
            double avg =  ((double) ov+ (double) ot)/2.0f;
            if (Math.abs( (double) ov- (double) ot)<0.001*avg){
                return true;
            } else {
                LOGGER.warn(" Double value mismatch "+ov+"!="+ot);
                return false;
            }
        } else if ( !( (ov==null && ot==null ) || ov.equals( ot) )){
            LOGGER.warn(" test failed "+f+", "+ov+" != "+ot);
            eq = false;
        }
        return eq;
    }

}
