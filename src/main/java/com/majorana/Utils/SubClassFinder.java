package com.majorana.Utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.util.Set;
import java.util.stream.Collectors;

public class SubClassFinder<T> {

    private static final MethodPrefixingLogger LOGGER = MethodPrefixingLoggerFactory.getLogger(SubClassFinder.class);

    public SubClassFinder(){

    }

    public Set<T> getSubclassInstances( String packageName, Class sup){

        Set<Class> packClasses = findSubclasses( packageName, sup);
        Set<T> filtered = packClasses.stream()
                .map( cl-> getInst(cl))
                .filter(inst -> inst!=null)
                .collect(Collectors.toSet());
        return filtered;

    }

    private T getInst(Class cl ){
        try {
            Constructor con = cl.getDeclaredConstructor();
            return (T) con.newInstance();
        } catch (Exception e){
            LOGGER.warn("getInst Class "+cl.getCanonicalName()+" has no parameter constructors");
        }
        return null;
    }

    public static Set<Class> findSubclasses( String packageName, Class sup){
        Set<Class> packClasses = findAllClassesUsingClassLoader(packageName);
        Set<Class> filtered = packClasses.stream()
                .filter( x-> sup.isAssignableFrom(x))
                .collect(Collectors.toSet());
        return filtered;
    }

    public static Set<Class> findAllClassesUsingClassLoader(String packageName) {
        InputStream stream = ClassLoader.getSystemClassLoader()
                .getResourceAsStream(packageName.replaceAll("[.]", "/"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        return reader.lines()
                .filter(line -> line.endsWith(".class"))
                .map(line -> getClass(line, packageName))
                .collect(Collectors.toSet());
    }

    private static Class getClass(String className, String packageName) {
        try {
            return Class.forName(packageName + "."
                    + className.substring(0, className.lastIndexOf('.')));
        } catch (ClassNotFoundException e) {
            // handle the exception
        }
        return null;
    }
}

