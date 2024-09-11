package com.majorana.Utils;

import com.majorana.ORM_ACCESS.DbBean;
import com.majorana.ORM.BaseMajoranaEntity;
import com.majorana.ORM.MajoranaAnnotationRepository;
import com.majorana.ORM.MajoranaRepositoryField;
import com.google.common.base.Objects;
import com.google.common.reflect.TypeToken;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class RandomEntity<T extends BaseDistillerEntity> {

    private static final MethodPrefixingLogger LOGGER = MethodPrefixingLoggerFactory.getLogger(RandomEntity.class);

    private List<MajoranaRepositoryField> mrf;

    private TypeToken<T> type = new TypeToken<T>(getClass()) {
    };

    private T value;


    private DbBean database;



    public RandomEntity(DbBean database) {
        super();
        this.database = database;
        try {
            value = (T) type.constructor(type.getRawType().getConstructor()).invoke(null);
//            value = (T) ((Class) ((ParameterizedType) this.getClass().
//                    getGenericSuperclass()).getActualTypeArguments()[0]).newInstance();
            Random r = new Random();
            List<MajoranaRepositoryField> mrf = MajoranaAnnotationRepository.getRepositoryFields(value.getClass());
            this.mrf = mrf;
            MajoranaAnnotationRepository.setRandom(mrf, value, r);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            LOGGER.warn(" Exception create new RandomEntity for type " + type.getRawType().getCanonicalName());
        }
    }

    public RandomEntity(DbBean database, Class clazz) {
        super();
        this.database = database;
        try {
            value = (T) clazz.getConstructor().newInstance();
//            value = (T) ((Class) ((ParameterizedType) this.getClass().
//                    getGenericSuperclass()).getActualTypeArguments()[0]).newInstance();
            Random r = new Random();
            List<MajoranaRepositoryField> mrf = MajoranaAnnotationRepository.getRepositoryFields(value.getClass());
            this.mrf = mrf;
            MajoranaAnnotationRepository.setRandom(mrf, value, r);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            LOGGER.warn(" Exception create new RandomEntity for type " + type.getRawType().getCanonicalName());
        } catch (InstantiationException e) {
            LOGGER.warn(" Exception create new RandomEntity for type " + type.getRawType().getCanonicalName());
        }
    }

//    public RandomEntity(Class S exte) {
//        super();
//        try {
//            BaseDistillerEntity sI = S.newInstance() ;
//
//                    //((Class)((ParameterizedType) S.
//                    //getGenericSuperclass()).getActualTypeArguments()[0]).newInstance();
//            if ( ! (c.isInstance(sI) )) {
//                LOGGER.warn(" Exception create new RandomEntity for type "+S.getCanonicalName());
//                return;
//            }
//            value = (T) sI;
//        Random r = new Random();
//        List<MajoranaRepositoryField> mrf = MajoranaAnnotationRepository. getRepositoryFields( S);
//        MajoranaAnnotationRepository.setRandom(mrf, value, r);
//        } catch (InstantiationException | IllegalAccessException e ){
//            LOGGER.warn(" Exception create new RandomEntity for type "+c.getCanonicalName());
//        }
//
//    }


    public T getValue(){
        return value;
    }

    public boolean isEqual(T test){
        return Objects.equal(value, test);
    }

    public List<String> diffForCreateFields(T test){
        List<String> failed = new LinkedList<>();
        Class c =type.getRawType();
        for(MajoranaRepositoryField f : mrf){
            if (f.getName().equals("deletedAt")) continue;
            if (!f.isId() && !f.isPopulatedCreated() && !f.isTransient() && !f.isPopulatedUpdated() && !f.isPopulatedCreated()){
                try {

                    if ( (value==null || test==null)
                      ) {
                        if (value == test) {
                            continue;
                        } else {
                            failed.add(f.getName());
                        }
                    }

                    Object ov = invokeGetter(value, f.getGetter());
                    Object ot = invokeGetter(test, f.getGetter());


                    if (ov instanceof java.util.Date && ot instanceof java.util.Date){
                       if (Math.abs( ((java.util.Date) ot).getTime() - ((java.util.Date) ov).getTime())>60000){
                            LOGGER.warn("Date value mismatch "+ov+"!="+ot);
                           failed.add(f.getName());
                        }
                    } else if (ov instanceof Float && ot instanceof Float){
                        double avg =  ((float) ov+ (float) ot)/2.0d;
                        if (Math.abs( (float) ov- (float) ot)<0.001 *avg){
                            continue;
                        }else {
                            LOGGER.warn("Float value mismatch "+ov+"!="+ot);
                            failed.add(f.getName());
                        }
                    } else if (ot instanceof Double && ot instanceof Double){
                        double avg =  ((double) ov+ (double) ot)/2.0f;
                        if (Math.abs( (double) ov- (double) ot)<0.001*avg){
                            continue;
                        } else {
                            LOGGER.warn(" Double value mismatch "+ov+"!="+ot);
                            failed.add(f.getName());
                        }
                    } else if ( !( (ov==null && ot==null ) || ov.equals( ot) )){
                        LOGGER.warn(" test failed "+f+", "+ov+" != "+ot);
                        failed.add(f.getName());
                    }
                } catch ( IllegalAccessException | IllegalArgumentException | InvocationTargetException e){
                    LOGGER.warn(" Exception testing created equals for  type "+c.getCanonicalName());
                    failed.add(f+" "+e.getMessage());
                }
            }
        }
        return failed;
    }


    public boolean isEqualForUpdateFields(T testUpdated , T testCreated){
        boolean eq = true;
        Class c = type.getRawType();
        for(MajoranaRepositoryField f : mrf){
            if (!f.isId() && !f.isPopulatedCreated() && !f.isPopulatedUpdated() && !f.isTransient()){
                try {
                    Object ov = invokeGetter(value, f.getGetter());
                    Object ou = invokeGetter(testUpdated, f.getGetter());
                    Object oc = invokeGetter(testCreated, f.getGetter());

                    Object ot = f.isUpdateable() ? ou : oc;

                    if ( !( (ov==null && ot==null ) || ov.equals( ot) )){
                        eq = false;
                    }
                } catch ( IllegalAccessException | IllegalArgumentException | InvocationTargetException e){
                    LOGGER.warn(" Exception testing equals updated fields for type "+ c.getCanonicalName());
                }
            }
        }
        return eq;
    }



    public static Object invokeGetter(Object obj, Method getter) throws IllegalAccessException, IllegalArgumentException , InvocationTargetException
    {
        if (obj==null){
            LOGGER.warn(getter.getName()+" invoke on null obj");
            return null;
        }
        Object f = getter.invoke(obj);
        return f;
    }

}
