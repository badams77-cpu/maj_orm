package com.majorana.orm.MajORM.ORM;


import java.util.List;

public abstract class
AbstractRotatableKey<T> {

    public abstract List<T> getRows(int start, int limit, int keyId);

    public abstract int putRows( List<T> rows);

    public abstract T encrypt(T t, boolean useOldKey) throws CryptoException;

    public abstract T decrypt( T t, boolean useOldKey) throws CryptoException;

}
