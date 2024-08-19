package com.majorana.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

/**
 *  An Slf4 based logger, that inlines the function where the error occured
 *
 */

public class MethodPrefixingLogger implements Logger {

    private final Logger mainLogger;
    
    private final String targetClassName;
    
    protected MethodPrefixingLogger(Class target){
        mainLogger = LoggerFactory.getLogger(target);
        targetClassName = target.getName();
    }
    
    private String getPrefix(){
        Thread current = Thread.currentThread();
        StackTraceElement stack[] = current.getStackTrace();
        for(int i=0; i< stack.length; i++){
            StackTraceElement el = stack[i];
            String className = el.getClassName();
            if (className.equals("java.lang.Thread")){ continue; }
            if (className.contains(MethodPrefixingLogger.class.getName())){ continue; }
            if (className.equals(targetClassName)){
                return el.getMethodName()+": ";
            }
            return "Logging Error: Unexcepted class: "+className+" using Logger for "+targetClassName+": ";
        }
        return "MethodPrefixingLogger: getPrefix: Empty Call Stack: ";
    }

    @Override
    public String getName() {
        return mainLogger.getName();
    }

    @Override
    public boolean isTraceEnabled() {
        return mainLogger.isTraceEnabled();
    }

    @Override
    public void trace(String msg) {
        mainLogger.trace(getPrefix()+msg);
    }

    @Override
    public void trace(String format, Object arg) {
        mainLogger.trace(getPrefix()+format, arg);
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        mainLogger.trace(getPrefix()+format, arg1, arg2);
    }

    @Override
    public void trace(String format, Object... arguments) {
        mainLogger.trace(getPrefix()+format, arguments);
    }

    @Override
    public void trace(String msg, Throwable t) {
        mainLogger.trace(getPrefix()+msg, t);
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return mainLogger.isTraceEnabled(marker);
    }

    @Override
    public void trace(Marker marker, String msg) {
        mainLogger.trace(marker, getPrefix()+msg);
    }

    @Override
    public void trace(Marker marker, String format, Object arg) {
        mainLogger.trace(marker, getPrefix()+format, arg);
    }

    @Override
    public void trace(Marker marker, String format, Object arg1, Object arg2) {
        mainLogger.trace(marker, getPrefix()+format, arg1, arg2);
    }

    @Override
    public void trace(Marker marker, String format, Object... argArray) {
        mainLogger.trace(marker, getPrefix()+format, argArray);
    }

    @Override
    public void trace(Marker marker, String msg, Throwable t) {
        mainLogger.trace(marker, getPrefix()+msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
        return mainLogger.isTraceEnabled();
    }

    @Override
    public void debug(String msg) {
        mainLogger.debug(getPrefix()+msg);
    }

    @Override
    public void debug(String format, Object arg) {
        mainLogger.debug(getPrefix()+format, arg);
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        mainLogger.debug(getPrefix()+format, arg1, arg2);
    }

    @Override
    public void debug(String format, Object... arguments) {
        mainLogger.debug(getPrefix()+format, arguments);
    }

    @Override
    public void debug(String msg, Throwable t) {
        mainLogger.debug(getPrefix()+msg, t);
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return mainLogger.isTraceEnabled(marker);
    }

    @Override
    public void debug(Marker marker, String msg) {
        mainLogger.debug(marker, getPrefix()+msg);
    }

    @Override
    public void debug(Marker marker, String format, Object arg) {
        mainLogger.debug(marker, getPrefix()+format, arg);
    }

    @Override
    public void debug(Marker marker, String format, Object arg1, Object arg2) {
        mainLogger.debug(marker, getPrefix()+format, arg1, arg2);
    }

    @Override
    public void debug(Marker marker, String format, Object... argArray) {
        mainLogger.debug(marker, getPrefix()+format, argArray);
    }

    @Override
    public void debug(Marker marker, String msg, Throwable t) {
        mainLogger.debug(marker, getPrefix()+msg, t);
    }

    @Override
    public boolean isInfoEnabled() {
        return mainLogger.isTraceEnabled();
    }


    @Override
    public void info(String msg) {
        mainLogger.info(getPrefix()+msg);
    }

    @Override
    public void info(String format, Object arg) {
        mainLogger.info(getPrefix()+format, arg);
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        mainLogger.info(getPrefix()+format, arg1, arg2);
    }

    @Override
    public void info(String format, Object... arguments) {
        mainLogger.info(getPrefix()+format, arguments);
    }

    @Override
    public void info(String msg, Throwable t) {
        mainLogger.info(getPrefix()+msg, t);
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return mainLogger.isTraceEnabled(marker);
    }

    @Override
    public void info(Marker marker, String msg) {
        mainLogger.info(marker, getPrefix()+msg);
    }

    @Override
    public void info(Marker marker, String format, Object arg) {
        mainLogger.info(marker, getPrefix()+format, arg);
    }

    @Override
    public void info(Marker marker, String format, Object arg1, Object arg2) {
        mainLogger.info(marker, getPrefix()+format, arg1, arg2);
    }

    @Override
    public void info(Marker marker, String format, Object... argArray) {
        mainLogger.info(marker, getPrefix()+format, argArray);
    }

    @Override
    public void info(Marker marker, String msg, Throwable t) {
        mainLogger.info(marker, getPrefix()+msg, t);
    }

    @Override
    public boolean isWarnEnabled() {
        return mainLogger.isTraceEnabled();
    }

    @Override
    public void warn(String msg) {
        mainLogger.warn(getPrefix()+msg);
    }

    @Override
    public void warn(String format, Object arg) {
        mainLogger.warn(getPrefix()+format, arg);
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        mainLogger.warn(getPrefix()+format, arg1, arg2);
    }

    @Override
    public void warn(String format, Object... arguments) {
        mainLogger.warn(getPrefix()+format, arguments);
    }

    @Override
    public void warn(String msg, Throwable t) {
        mainLogger.warn(getPrefix()+msg, t);
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return mainLogger.isTraceEnabled(marker);
    }

    @Override
    public void warn(Marker marker, String msg) {
        mainLogger.warn(marker, getPrefix()+msg);
    }

    @Override
    public void warn(Marker marker, String format, Object arg) {
        mainLogger.warn(marker, getPrefix()+format, arg);
    }

    @Override
    public void warn(Marker marker, String format, Object arg1, Object arg2) {
        mainLogger.warn(marker, getPrefix()+format, arg1, arg2);
    }

    @Override
    public void warn(Marker marker, String format, Object... argArray) {
        mainLogger.warn(marker, getPrefix()+format, argArray);
    }

    @Override
    public void warn(Marker marker, String msg, Throwable t) {
        mainLogger.warn(marker, getPrefix()+msg, t);
    }




    @Override
    public boolean isErrorEnabled() {
        return mainLogger.isTraceEnabled();
    }

    @Override
    public void error(String msg) {
        mainLogger.error(getPrefix()+msg);
    }

    @Override
    public void error(String format, Object arg) {
        mainLogger.error(getPrefix()+format, arg);
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        mainLogger.error(getPrefix()+format, arg1, arg2);
    }

    @Override
    public void error(String format, Object... arguments) {
        mainLogger.error(getPrefix()+format, arguments);
    }

    @Override
    public void error(String msg, Throwable t) {
        mainLogger.error(getPrefix()+msg, t);
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return mainLogger.isTraceEnabled(marker);
    }

    @Override
    public void error(Marker marker, String msg) {
        mainLogger.error(marker, getPrefix()+msg);
    }

    @Override
    public void error(Marker marker, String format, Object arg) {
        mainLogger.error(marker, getPrefix()+format, arg);
    }

    @Override
    public void error(Marker marker, String format, Object arg1, Object arg2) {
        mainLogger.error(marker, getPrefix()+format, arg1, arg2);
    }

    @Override
    public void error(Marker marker, String format, Object... argArray) {
        mainLogger.error(marker, getPrefix()+format, argArray);
    }

    @Override
    public void error(Marker marker, String msg, Throwable t) {
        mainLogger.error(marker, getPrefix()+msg, t);
    }




}
