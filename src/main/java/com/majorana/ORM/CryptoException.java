package Distiller.ORM;

/**
 *  An exception indicating a encrypted database in the database was not able ot be encrypted or
 *  decrypted
 */

public class CryptoException extends RuntimeException {

    public CryptoException(String message){
        super(message);
    }

    public CryptoException(String message, Exception e){
        super(message,e);
    }
}
