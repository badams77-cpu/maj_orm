package com.majorana.Utils;

import com.majorana.persist.newannot.EntityPackage;
import com.majorana.entities.ex2.DirEntityVersion;
import org.apache.commons.lang3.tuple.Pair;
import org.reflections.Reflections;

import org.reflections.scanners.FieldAnnotationsScanner;
import org.reflections.scanners.Scanners;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.*;
import java.io.*;
import java.util.stream.Collectors;

public class SubClassFinder<T> {

    private static final MethodPrefixingLogger LOGGER = MethodPrefixingLoggerFactory.getLogger(SubClassFinder.class);

    private int minMajor;
    private int minMinor;
    private Pair<Integer, Integer> version;

    private static final String[] systemPackagesPrefixes = {"java","javax"};

    public SubClassFinder(int major, int minor) {
        this.minMajor = major;
        this.minMinor = minor;
        version = Pair.of(major, minor);
    }

    public SubClassFinder() {

    }

    public Set<T> getSubclassInstances(String packageName, Class sup) {

        Set<Class> packClasses = findSubclasses(packageName, sup);
        PairComparator pc = new PairComparator();
        Set<T> filtered = packClasses.stream()
                .filter( cl-> pc.compare(version, getEntityVersions(cl))>=0)
                .map(cl -> getInst(cl))
                .filter(inst -> inst != null)

                .collect(Collectors.toSet());
        return filtered;

    }

    public Set<T> getSubclassInstancesInPackagePrefix( String packagePrefix, Class sup) {
        PairComparator pc = new PairComparator();
        Set<Class> packClasses = findSubclassesInPackagePrefix(packagePrefix,  sup);
        Set<T> filtered = packClasses.stream()
                .filter( cl-> pc.compare(version, getEntityVersions(cl))>=0)
                .map(cl -> getInst(cl))
                .filter(inst -> inst != null)
                .collect(Collectors.toSet());
        return filtered;

    }

    private T getInst(Class cl) {
        try {
            Constructor con = cl.getDeclaredConstructor();
            return (T) con.newInstance();
        } catch (Exception e) {
            LOGGER.warn("getInst Class " + cl.getCanonicalName() + " has no parameter constructors");
        }
        return null;
    }


/*
    public static Set<Class> findSubclasses( Class sup) {
        Set<Class> packClasses = findAllClassesUsingReflectionsLibrary();
       Set<Class> filtered = packClasses.stream()
                .filter(x -> sup.isAssignableFrom(x))
                .collect(Collectors.toSet());
        return filtered;
    }
*/
    public Set<Class> findSubclassesInPackagePrefix(String packagePrefix, Class sup){
        List<String> packages = getPackages();
        List<String> goodPackages = packages.stream().filter(p->p.startsWith(packagePrefix)).collect(Collectors.toUnmodifiableList());
        Set<Class> results =new HashSet<>();
        for(String pack : goodPackages) {
            Set<Class> goodCLasses = findSubclassesInPack(pack, sup);
            results.addAll(goodCLasses);
        }
        return results;
    }

    public Set<Class> findSubclassesInPackagePrefix(String packagePrefix, Class sup, int major, int minor){
        List<String> packages = getPackages();
        List<String> prefixPackages = packages.stream().filter(p->p.startsWith(packagePrefix)).collect(Collectors.toUnmodifiableList());
        List<String> goodPackages = prefixPackages.stream().map(p-> Pair.of(p, getHighestVersionInPackage(p)))
                .filter( pair->pair.getRight().getLeft()< major || (pair.getRight().getLeft()==major &&
                        pair.getRight().getRight()>=minor)).map(p->p.getLeft()).collect(Collectors.toUnmodifiableList());

        Set<Class> results =new HashSet<>();
        for(String pack : goodPackages) {
            Set<Class> goodCLasses = findSubclassesInPack(pack, sup);
            results.addAll(goodCLasses);
        }
        return results;
    }

    public  Set<Class> findSubclassesInPack(String pack, Class sup){
        Set<Class> packClasses = findAllClassesInPackage(pack);
        Set<Class> filtered = packClasses.stream()
                .filter(x -> sup.isAssignableFrom(x))
                .collect(Collectors.toSet());
        return filtered;
    }

    public Set<Class> findAllClassesInPackage(String package1){
        List<ClassLoader> classLoadersList = getLoaders();
        Set<Class> classes = new HashSet<Class>();
        for(ClassLoader lo : classLoadersList){
            Set<Class> loClasses = findAllClassesUsingClassLoader1(package1, lo);
            classes.addAll(loClasses);
        }
        return classes;
    }

    public Set<Class> findAllClassesUsingClassLoader1(String packageName, ClassLoader loader) {
        InputStream stream = loader
                .getResourceAsStream(packageName.replaceAll("[.]", "/"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        return reader.lines()
                .filter(line -> line.endsWith(".class"))
                .map(line -> getClass(line, packageName))
                .collect(Collectors.toSet());
    }

    public List<ClassLoader> getLoaders(){
        List<ClassLoader> classLoadersList = Thread.getAllStackTraces().keySet() //Get all active threads
                .stream()
                .map(thread -> thread.getContextClassLoader()) //Get the classloader of the thread (may be null)
                .filter(p -> p != null) //Filter out every null object from the stream
                .collect(Collectors.
                        toCollection(ArrayList::new));
        return classLoadersList;
    }


    public static Reflections getReflections() {
        try {
            List<ClassLoader> classLoadersList = Thread.getAllStackTraces().keySet() //Get all active threads
                    .stream()
                    .map(thread -> thread.getContextClassLoader()) //Get the classloader of the thread (may be null)
                    .filter(p -> p != null) //Filter out every null object from the stream
                    .collect(Collectors.
                            toCollection(ArrayList::new));

            classLoadersList.add(  ClassLoader.getPlatformClassLoader());
            Collection<URL> allPackagePrefixes = Arrays.stream(Package.getPackages()).map(p -> p.getName())
                    .map(s -> s.split("\\.")[0]).distinct().map(s -> ClasspathHelper.forPackage(s)).reduce((c1, c2) -> {
                        Collection<URL> c3 = new HashSet<>();
                        c3.addAll(c1);
                        c3.addAll(c2);
                        return c3;
                    }).get();
            ConfigurationBuilder config = new ConfigurationBuilder().addUrls(allPackagePrefixes).addScanners(
                    new SubTypesScanner()
                //    new TypeAnnotationsScanner(),
                //    new FieldAnnotationsScanner()
            ) .setClassLoaders(classLoadersList.toArray(new ClassLoader[0]));
            Reflections reflections = new Reflections(config);


        return reflections;
    } catch (Exception e){
            LOGGER.warn("Error in finding reflection",e);
        }
        return null;
    }

    public static Reflections getReflections(Class c) {
        try {
            List<ClassLoader> classLoadersList = Thread.getAllStackTraces().keySet() //Get all active threads
                    .stream()
                    .map(thread -> thread.getContextClassLoader()) //Get the classloader of the thread (may be null)
                    .filter(p -> p != null) //Filter out every null object from the stream
                    .collect(Collectors.toCollection(ArrayList::new));

            classLoadersList.add(  ClassLoader.getPlatformClassLoader());
            Collection<URL> allPackagePrefixes = Arrays.stream(Package.getPackages()).map(p -> p.getName())
                    .map(s -> s.split("\\.")[0]).distinct()
                    .map(s -> ClasspathHelper.forPackage(s)).reduce((c1, c2) -> {
                        Collection<URL> c3 = new HashSet<>();
                        c3.addAll(c1);
                        c3.addAll(c2);
                        return c3;
                    }).get();
            ConfigurationBuilder config = new ConfigurationBuilder().addUrls(allPackagePrefixes).addScanners(
                    Scanners.SubTypes.filterResultsBy( s-> s.getClass().isAssignableFrom(c))
            ) .setClassLoaders(classLoadersList.toArray(new ClassLoader[0]));
            Reflections reflections = new Reflections(config);


            return reflections;
        } catch (Exception e){
            LOGGER.warn("Error in finding reflection",e);
        }
        return null;
    }

    public static Set<Class> findSubclasses(String packageName, Class sup) {
        Set<Class> packClasses = findNamedClassesUsingReflectionsLibrary(packageName+".", sup);
        Set<Class> filtered = packClasses.stream()
         //       .filter(x -> sup.isAssignableFrom(x))
                .collect(Collectors.toSet());
        return filtered;
    }

    public static Reflections getReflections(String packageName){

        List<ClassLoader>  classLoadersList = Thread.getAllStackTraces().keySet() //Get all active threads
                .stream()
                .map(thread -> thread.getContextClassLoader()) //Get the classloader of the thread (may be null)
                .filter(p->p!=null) //Filter out every null object from the stream
                .collect(Collectors.toCollection(ArrayList::new));
        classLoadersList.add(  ClassLoader.getPlatformClassLoader());

        ConfigurationBuilder config = new ConfigurationBuilder().addScanners(
                new TypeAnnotationsScanner(),
                new FieldAnnotationsScanner()
                ).forPackages(packageName)
                .setClassLoaders(classLoadersList.toArray(new ClassLoader[0]));

        try {
            //new SubTypesScanner(false /* don't exclude Object.class */)
            Reflections reflections = new Reflections(config);
            return reflections;
        } catch (Exception e){
            LOGGER.warn("Error in finding reflection",e);
        }
        return null;
    }
//    let’s retrieve all the functional interfaces of the  package:

    public Set<Class> getAnnotatedClassInPackage(String pack, Class anno) {
         Set<Class> ret = new HashSet<>();
         Set<Class> classPack =  findAllClassesInPackage(pack);
         for(Class c : classPack){
             if (c.getAnnotationsByType(anno).length>0){
                 ret.add(c);
             }
         }
        return ret;
    }

    public Pair<Integer, Integer> getHighestVersionInPackage(String pack){
        int major = 0;
        int minor = 0;
        Set<Class> entityPackages = getAnnotatedClassInPackage(pack, EntityPackage.class);
        Comparator<Pair<Integer,Integer>> comp =  new PairComparator();
        List<Class> res = entityPackages.stream().sorted( (a,b)->comp.compare( getEntityVersions(b), getEntityVersions(a) )).collect(Collectors.toList());
        if (res.size()==0){ return Pair.of(0,0); }
        Pair<Integer, Integer> highVersion = getEntityVersions(res.get(0));
        return highVersion;
    }

    public Pair<Integer, Integer> getEntityVersions(Class cl){
        Optional<Annotation> annOpt = java.util.Arrays.stream(cl.getAnnotationsByType(EntityPackage.class)).findFirst();
        if (annOpt.isEmpty()){ return Pair.of(0,0); }
        EntityPackage pack = (EntityPackage) annOpt.get();
        return Pair.of(pack.major(), pack.minor());
    }

    public Pair<Integer, Integer> getCLassEnitityVersion(Class clazz) {
        PairComparator comp = new PairComparator();
        List<EntityPackage> nep = Arrays.stream(clazz.getAnnotations()).filter(x->((Annotation) x) instanceof EntityPackage)
                .sorted( (a,b)-> comp.compare( getPair((EntityPackage) a), getPair((EntityPackage) b)))
                .map( a-> (EntityPackage) a)
                .collect(Collectors.toList());
        if (nep.isEmpty()){ return Pair.of(0,0); }
        EntityPackage ep = nep.get(0);
        return Pair.of(ep.major(), ep.minor());
    }

    public Pair<Integer, Integer> getPair(EntityPackage ep){
        return Pair.of(ep.major(), ep.minor());
    }

    public static Set<Class> findAllClassesUsingReflectionsLibrary(String packageName) {
        Reflections reflections = getReflections(packageName);
        return reflections.getSubTypesOf(Object.class)
                .stream()
                .collect(Collectors.toSet());
    }

    public static Set<Class> findNamedClassesUsingReflectionsLibrary(String packageName, Class sup) {
        Reflections reflections = getReflections(packageName);
        return reflections.getSubTypesOf(sup);
    //            .stream().map(x-> (Class) x)
    //            .collect(Collectors.toSet());
    }

    public static Set<Class> findAllClassesUsingReflectionsLibrary() {
        Reflections reflections = getReflections();
        Set<Class> set = reflections.getSubTypesOf(Object.class)
                .stream()
                .collect(Collectors.
                        toSet());
        return set;
    }

    public static Set<Class> findAllClassesUsingReflectionsLibrarySUbclassing(Class c) {
        Reflections reflections = getReflections(c);
        Set<Class> set = reflections.get(Scanners.SubTypes.of(c).asClass())
                .stream()
                .collect(Collectors.toSet());
        return set;
    }

    // Not working
    public static Set<Class> findAllClassesUsingClassLoader(String packageName) {
        InputStream stream = ClassLoader.getSystemClassLoader()
                .getResourceAsStream(packageName.replaceAll("[\\.]", "/"));
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
            LOGGER.warn("Class "+className+" in package "+packageName+" not found");
        }
        return null;
    }


    public List<String> getPackages(){

        List<String> result = new LinkedList<String>();

        List<Package> packages = Arrays.stream(Package.getPackages()).collect(Collectors.toList());

        for(Package pack : packages) {
            int n = 0;
            for (Package p : packages) {
                boolean isSystemPackage = false;
                String pname = p.getName();
                for (int j = 0; j < systemPackagesPrefixes.length; j++) {
                    String spname = systemPackagesPrefixes[j];
                    if (pname.startsWith(spname)) {
                        isSystemPackage = true;
                        break;
                    } else {
                        result.add(p.getName());
                    }
                }
            }
        }
        return result;
    }
}

