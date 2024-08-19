package Distiller.ORM;

/**
 *  Helper Abstract CLass for Rotatable Crypto keys for keeping sensitive data in the database
 *  with the crypto key being regularly rotated
 */

import java.util.*;

public abstract class AbstractRotatableKey<T> {

    public abstract List<T> getRows(int start, int limit, int keyId);

    public abstract int putRows( List<T> rows);

    public abstract T encrypt(T t, boolean useOldKey) throws CryptoException;

    public abstract T decrypt( T t, boolean useOldKey) throws CryptoException;

}
