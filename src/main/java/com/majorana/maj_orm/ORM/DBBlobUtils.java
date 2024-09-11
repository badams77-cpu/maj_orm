package com.majorana.ORM;

import com.majorana.Utils.MethodPrefixingLoggerFactory;
import org.slf4j.Logger;

import com.majorana.persist.newannot.*;

import javax.sql.rowset.serial.SerialBlob;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Blob;

/**
 * THe is a help class for storing blob data in the database
 */

public class DBBlobUtils {

    private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(DBBlobUtils.class);


    /**
     *
     * @param object - converts a java object to Blob in read for the  database
     * @return blob,
     */

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

    /**
     *
     * @param blob - converts a blob from the dayabase to a java object
     * @return
     */

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