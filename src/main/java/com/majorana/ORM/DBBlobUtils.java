package Distiller.ORM;

import Distiller.Utils.MethodPrefixingLoggerFactory;
import org.slf4j.Logger;

import javax.sql.rowset.serial.SerialBlob;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Blob;

public class DBBlobUtils {

    private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(DBBlobUtils.class);
    public static Blob writeJavaObject( Object object) {
        Blob empty = null;
        try {
            empty = new SerialBlob(new byte[0]);
           String className = object.getClass().getName();
            ByteArrayOutputStream bis = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bis);
            oos.writeObject(object);
            oos.close();
            bis.close();
            Blob blob = new SerialBlob(bis.toByteArray());
            return blob;
        } catch (Exception e) {
            LOGGER.warn("Error deserialising blog");
            return empty;
        }
    }

    public static Object readJavaObject( Blob blob) {
        try {
            ObjectInputStream ois = new ObjectInputStream(blob.getBinaryStream());
            Object object = ois.readObject();
            String className = object.getClass().getName();
            ois.close();
            return object;
        } catch (Exception e){
            LOGGER.warn("Error deserialising blog");
        }
        return null;
    }


}
