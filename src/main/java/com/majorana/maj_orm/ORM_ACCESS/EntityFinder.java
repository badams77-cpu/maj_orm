package com.majorana.maj_orm.ORM_ACCESS;

import com.majorana.maj_orm.Utils.MethodPrefixingLoggerFactory;
import com.majorana.maj_orm.Utils.SubClassFinder;
import com.majorana.maj_orm.ORM.BaseMajoranaEntity;
import com.majorana.maj_orm.newannot.EntityPackage;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.stream.Collectors;

import org.reflections.Reflections;
import org.slf4j.Logger;

public class EntityFinder {

    private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(DbBean.class);

    private EntityPackage emptyPackage = new EntityPackage(){
        @Override
        public Class<? extends Annotation> annotationType() {
            return EntityPackage.class;
        }

        @Override
        public int minor() {
            return 0;
        }

        @Override
        public int major() {
            return 0;
        }
    };

    private Class entityClass = null;

    private Class entityPackage = null;

    private Map<String, Class> entities = new HashMap<>();

    private Map<Class, String> reverse = new HashMap<>();
    
    private int major=1;
    private int minor=1;

    private static final String[] systemPackagesPrefixes = {"java","javax"};

    private String packagePrefix = "";

    public EntityFinder(){
      //  entityClass = jakarta.persistence.Entity.class;
        entityClass = BaseMajoranaEntity.class;
        entityPackage = EntityPackage.class;
        entities = new HashMap<String, Class>();
        reverse = new HashMap<Class, String>();
    }

    public EntityFinder(Class entityClass, Class entityPackageVersion, String packagePrefix){
        this.entityClass = entityClass;
        this.entityPackage = entityPackage;
        this.packagePrefix = packagePrefix;
        entities = new HashMap<String, Class>();
        reverse = new HashMap<Class, String>();
    }
    
    public void setAllowedVersions(int major, int minor ){
        this.major = major;
        this.minor = minor;
    }

    public Class getClassForName(String name){
        return entities.getOrDefault(name, null);
    }

    public String getNameForClass(Class name){
        return reverse.getOrDefault(name, null);
    }

    public Set<BaseMajoranaEntity> getEntities(String packagePrefix){
        Set<BaseMajoranaEntity> entityTypes = getEntitiesForPackagePrefix(packagePrefix).stream().
                map( p->
                {try {
                    return p.newInstance();
                } catch (Exception e){ return null;}})
                .filter(p->p!=null)
                .map(p-> (BaseMajoranaEntity) p)
                .collect(Collectors.toSet());
        return entityTypes;
    }



    public List<Pair<String, Class>> getValidEntities(){
        List<Pair<String, Class>> valid = new LinkedList<>();
        for(Map.Entry<String, Class> entry : entities.entrySet()){
            valid.add(Pair.of(entry.getKey(), entry.getValue()));
        }
        return valid;
    }

    public void populateEntity(boolean checkPackageForEntityVersion){
        List<String> packages = getPackages();
        entities = new HashMap<String, Class>();
        reverse = new HashMap<Class, String>();
        List<String> pack = new LinkedList<>();
        if (checkPackageForEntityVersion) {
            pack = packages.stream().filter(p -> {
                Pair<Integer, Integer> pair = packageVersion(p);
                if (pair == null) {
                    return false;
                }
                int packMaj = pair.getLeft();
                int packMin = pair.getRight();
                if (packMaj == 0 && packMin == 0) {
                    return true;
                }
                if (packMaj < major) {
                    return true;
                }
                if (packMaj == major && packMaj < minor) {
                    return true;
                }
                return false;
            }).collect(Collectors.toList());
        }  else {
            pack = getPackages();
        }
        for(String pak : pack){
            Set<Class> entitiesAndNames = getEntitiesForPackage(pak);
            for(Class datum : entitiesAndNames){
                String name = datum.getCanonicalName();
                Class clazz =datum;
                entities.put(name, clazz);
                reverse.put(clazz, name);
            }
            
        }
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



    public Set<Class> getEntitiesForPackagePrefix(String pack) {

//        Reflections reflections = new Reflections(pack);

        //      Set<Class<?>> types = reflections
        //            .getSubTypesOf(entityClass);

        SubClassFinder finder = new SubClassFinder();
        Set<Class> types = finder.findSubclassesInPackagePrefix(pack, entityClass);

        return (Set<Class>) types;
    }


    public Set<Class> getEntitiesForPackage(String pack) {

//        Reflections reflections = new Reflections(pack);

        //      Set<Class<?>> types = reflections
        //            .getSubTypesOf(entityClass);

        SubClassFinder finder = new SubClassFinder();
        Set<Class> types = finder.findSubclasses(pack, entityClass);

        return (Set<Class>) types;
    }


    public List<Pair<Class, String>> getAnnotatedEntitiesForPackage(String pack) {

        Reflections reflections = new Reflections(pack);

        Set<Class<?>> types = reflections
                .getSubTypesOf(entityClass);
        List<Pair<Class, String>> results = types.stream()
                .map(clazz -> Pair.of((Class) clazz.getClass(), clazz.getCanonicalName()))
                .map( pa -> pa.getRight()==null || pa.getRight().equals("") ? Pair.of(pa.getLeft(), pa.getLeft().getCanonicalName()) : pa)
                .collect(Collectors.toList()) ;
        
        return results;
    }

    public Pair<Integer, Integer> packageVersion(String pack){
        Reflections reflections = new Reflections(pack);
        Set<Class<?>> types  = reflections.getTypesAnnotatedWith(entityPackage);
        Pair<Integer, Integer> packVersion = types.stream().
                map(x-> Arrays.stream(x.getAnnotationsByType(entityPackage)).findFirst().orElse(emptyPackage))
                .filter( a->a!=null)
                .map(a->((EntityPackage) a)).sorted (  Comparator.comparing( a->((EntityPackage) a).major())
                        .thenComparing(b-> ((EntityPackage) b).minor()).reversed())
                .map( c-> Pair.of( c.major(), c.minor()))
                   .findFirst().orElse(Pair.of(0,0));
        return packVersion;
    }

    private int majorComp(EntityPackage a, EntityPackage b){
        return Integer.compare(a.major() , b.major());
    }

    private int minorComp(EntityPackage a, EntityPackage b){
        return Integer.compare(a.minor() , b.minor());
    }

}
