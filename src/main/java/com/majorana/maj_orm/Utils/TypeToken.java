package com.majorana.maj_orm.Utils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Replaced with Guava
 *
 * @param <T>
 */

public abstract class TypeToken<T> {
    private Type type;

    public TypeToken(){
        Type superClass = getClass().getGenericSuperclass();
        this.type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
    }

    public Type getType() {
        return type;
    }

    public Class getTypeClass(){
        ParameterizedType genericSuperclass = (ParameterizedType)
                getClass().getGenericSuperclass();
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>)
                genericSuperclass.getActualTypeArguments()[0];
        return clazz;
    }

}