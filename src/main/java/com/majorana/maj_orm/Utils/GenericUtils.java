package com.majorana.maj_orm.Utils;


import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class GenericUtils {

    public GenericUtils() {
    }

    public static boolean isNullOrEmpty(String in) {
        return !isValid(in);
    }

    public static String ifNullOrEmpty(String in, String retIfNullOrEmpty) {
        return !isNullOrEmpty(in) ? in : getValid(retIfNullOrEmpty);
    }

    public static boolean isValid(String in) {
        boolean ret = false;

        try {
            if (in != null && !"".equals(in)) {
                ret = true;
            }

            return ret;
        } catch (Exception var3) {
            return ret;
        }
    }

    public static boolean isValid(BigDecimal in) {
        boolean ret = false;

        try {
            if (in != null && !Double.isNaN(in.doubleValue())) {
                ret = true;
            }

            return ret;
        } catch (Exception var3) {
            return ret;
        }
    }

    public static boolean isValid(List in) {
        boolean ret = false;

        try {
            if (in != null && !in.isEmpty()) {
                ret = true;
            }

            return ret;
        } catch (Exception var3) {
            return ret;
        }
    }

    public static boolean isValid(Map in) {
        boolean ret = false;

        try {
            if (in != null && !in.isEmpty()) {
                ret = true;
            }

            return ret;
        } catch (Exception var3) {
            return ret;
        }
    }

    public static boolean isBoolean(String in) {
        boolean ret = true;

        try {
            boolean var2 = Boolean.parseBoolean(in);
        } catch (Exception var3) {
            ret = false;
        }

        return ret;
    }

    public static boolean isInteger(String in) {
        boolean ret = true;

        try {
            Integer var2 = Integer.parseInt(in);
        } catch (Exception var3) {
            ret = false;
        }

        return ret;
    }

    public static boolean isLong(String in) {
        boolean ret = true;

        try {
            Long var2 = Long.parseLong(in);
        } catch (Exception var3) {
            ret = false;
        }

        return ret;
    }

    public static String getValid(String s) {
        String ret = "";
        if (s != null) {
            ret = s;
        }

        return ret;
    }

    public static int getRandomInt() {
        return (Double.valueOf(Math.random() * 100.0)).intValue();
    }

    public static double getRandomDouble() {
        return (double)(Double.valueOf(Math.random() * 100.0)).intValue();
    }

    public static boolean isNull(Object obj) {
        return obj == null;
    }
}
