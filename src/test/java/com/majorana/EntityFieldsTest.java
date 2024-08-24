


package com.majorana;

import com.majorana.Utils.*;

import com.majorana.ORM_ACCESS.*;

import com.com.majorana.ORM.BaseMajoranaEntity;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.LinkedList;

import java.util.List;
import java.util.Set;

import java.util.stream.Collectors;


import java.util.HashMap;
import java.util.Map;

//import org.junit.runners.Suite;
//import org.junit.platform.JUnitPlatform;

//import org.junit.runner.Runner;

import static org.junit.jupiter.api.Assertions.assertTrue;

//@RunWith(JUnitPlatform.class)
public class EntityFieldsTest {

    private static final MethodPrefixingLogger LOGGER = MethodPrefixingLoggerFactory.getLogger(EntityFieldsTest.class);

    private final static String PREFIX = "Majorana";

    private final static String DB_VERSION = "DBVersion";

    private final static String MAJOR = "Major";

    private final static String MINOR = "Minor";

    private final static String CHECK_DBVERSION = "CheckDbVersion";

    private static DbBean dbBean  = null;

    private static final String ENTITIES_DIR = "com.majorana.ORM";

    public EntityFieldsTest(){
        dbBean = DbBean.getSingletonLazy();
        try {
            dbBean.connect();
        } catch (Exception e){
            LOGGER.error("could not connect to db");
        }
    }


    public static void setup() throws Exception {
        dbBean = DbBean.getSingletonLazy();
        dbBean.connect();
    }

    @Test
    public void listField() {

          SubClassFinder finder = new SubClassFinder<BaseMajoranaEntity>();
        Set<BaseMajoranaEntity> entityTypes = finder.getSubclassInstances( BaseMajoranaEntity.class);
        for( BaseMajoranaEntity bde : entityTypes){
          System.err.println( " ---- ALL ---");
          System.err.println( bde.getClass().getCanonicalName());
          System.err.println( bde.getFields());
            LOGGER.warn( " ---- ");
            LOGGER.warn( bde.getClass().getCanonicalName());
            LOGGER.warn( bde.getFields());
        }
    }

    @Test
    public void listField_1_0() {
        SubClassFinder finder = new SubClassFinder<BaseMajoranaEntity>(1,0);
        Set<BaseMajoranaEntity> entityTypes = finder.getSubclassInstances( BaseMajoranaEntity.class);
        for( BaseMajoranaEntity bde : entityTypes){
            System.err.println( " ----  V1 ----");
            System.err.println( bde.getClass().getCanonicalName());
            System.err.println( bde.getFields());
            LOGGER.warn( " ---- ");
            LOGGER.warn( bde.getClass().getCanonicalName());
            LOGGER.warn( bde.getFields());
        }
    }

    @Test
    public void listField_2_0() {
        SubClassFinder finder = new SubClassFinder<BaseMajoranaEntity>(2,0);
        Set<BaseMajoranaEntity> entityTypes = finder.getSubclassInstances( BaseMajoranaEntity.class);
        for( BaseMajoranaEntity bde : entityTypes){
            System.err.println( " ---- V2 -----");
            System.err.println( bde.getClass().getCanonicalName());
            System.err.println( bde.getFields());
            LOGGER.warn( " ---- ");
            LOGGER.warn( bde.getClass().getCanonicalName());
            LOGGER.warn( bde.getFields());
        }
    }

    @Test
    public void dbSaveAndLoadRandom(){
        SubClassFinder finder = new SubClassFinder<BaseMajoranaEntity>();
        Set<Class> entityTypes = finder.findSubclasses(  BaseMajoranaEntity.class);
        int ok =0;
        List<String> good = new LinkedList<>();
        int failed = 0;
        List<String> bad = new LinkedList<>();
        for(Class et : entityTypes){
            try {
                RandomEntity<BaseMajoranaEntity> rbde = new RandomEntity<>(dbBean, et);
                Majorana.ORM_ACCESS.MultiId id = dbBean.storeBean(rbde.getValue());
                assertTrue(id.hasAnyId(),
                        "Storing " + et + " failed "
                );
                String paramNames[] = new String[1];
                paramNames[0] = "id";
                Object params[] = new Object[1];
                params[0] = id.getId();
                BaseMajoranaEntity tbde = (BaseMajoranaEntity) dbBean.getBeanNP(et, rbde.getValue().getTableName(), paramNames, params);
                assertTrue(rbde.isEqualForCreateFields(tbde),
                        "Stored fields not equals ");
                ok++;
                good.add( et.getCanonicalName());
            } catch (Exception e){
                bad.add(et.getCanonicalName());
                LOGGER.error("Failed for type "+et, e);
                failed++;
            }
        }
        LOGGER.warn("DB tests passed for "+ok+" classes "+good.stream().collect(Collectors.joining(", ")));
        LOGGER.warn("DB tests failed for "+failed+" classes "+bad.stream().collect(Collectors.joining(", ")));
    }


}
