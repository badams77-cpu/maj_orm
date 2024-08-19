package com.majorana.Utils;

import Majorana.ORM.MajoranaAnnotationRepository;
import Majorana.ORM.MajoranaRepositoryField;
import Majorana.entities.BaseMajoranaEntity;
import com.google.common.base.Objects;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Random;

/**
 * Constructs a random data amd place it in a an entity
 *
 *
 * @param <T>
 */


public class RandomEntity<T extends BaseMajoranaEntity> extends TypeToken<T> {

    private static final MethodPrefixingLogger LOGGER = MethodPrefixingLoggerFactory.getLogger(SubClassFinder.class);

    List<MajoranaRepositoryField> mrf;

    private T value;

    /**
     * Constructor
     */

    public RandomEntity() {
        super();
        Class c = getTypeClass();
        try {
            value = (T) ((Class) ((ParameterizedType) this.getClass().
                    getGenericSuperclass()).getActualTypeArguments()[0]).newInstance();
            Random r = new Random();
            ;
            List<MajoranaRepositoryField> mrf = MajoranaAnnotationRepository.getRepositoryFields(value.getClass());
            this.mrf = mrf;
            MajoranaAnnotationRepository.setRandom(mrf, value, r);
        } catch (InstantiationException | IllegalAccessException e) {
            LOGGER.warn(" Exception create new RandomENtity for type " + c.getCanonicalName());
        }
    }

    /**
     *  Constructor of type S
     * @param S
     */

    public RandomEntity(Class S){
        super();
        Class c = getTypeClass();
        try {
            Object sI =  ((Class)((ParameterizedType) S.
                    getGenericSuperclass()).getActualTypeArguments()[0]).newInstance();
            if ( ! (c.isInstance(sI) )) {
                LOGGER.warn(" Exception create new RandomENtity for type "+S.getCanonicalName());
                return;
            }
            value = (T) sI;
        Random r = new Random();
        List<MajoranaRepositoryField> mrf = MajoranaAnnotationRepository. getRepositoryFields( S);
        MajoranaAnnotationRepository.setRandom(mrf, value, r);
        } catch (InstantiationException | IllegalAccessException e ){
            LOGGER.warn(" Exception create new RandomENtity for type "+c.getCanonicalName());
        }

    }


    public T getValue(){
        return value;
    }

    public boolean isEqual(T test){
        return Objects.equal(value, test);
    }

    /**
     * Is the field emptu
     *
     * @param test
     * @return
     */

    public boolean isEqualForCreateFields(T test){
        boolean eq = true;
        Class c = getTypeClass();
        for(MajoranaRepositoryField f : mrf){
            if (!f.isId() && !f.isPopulatedCreated() && !f.isTransient()){
                try {
                    Object ov = invokeGetter(value, f.getGetter());
                    Object ot = invokeGetter(test, f.getGetter());

                    if ( !( (ov==null && ot==null ) || ov.equals( ot) )){
                        eq = false;
                    }
                } catch ( IllegalAccessException | IllegalArgumentException | InvocationTargetException e){
                    LOGGER.warn(" Exception testing created equals for  type "+c.getCanonicalName());
                }
            }
        }
        return eq;
    }

    /**
     * Is the fields equals for updatable fields
     *
     * @param testUpdated
     * @param testCreated
     * @return
     */

    public boolean isEqualForUpdateFields(T testUpdated , T testCreated){
        boolean eq = true;
        Class c = getTypeClass();
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
        Object f = getter.invoke(obj);
        return f;
    }

}
