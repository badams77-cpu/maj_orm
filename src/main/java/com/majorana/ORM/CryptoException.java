package Distiller.ORM;

public class CryptoException extends RuntimeException {

    public CryptoException(String message){
        super(message);
    }

    public CryptoException(String message, Exception e){
        super(message,e);
    }
}
