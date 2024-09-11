package Distiller;

import com.majorana.Utils.*;


import com.majorana.ORM.*;

import com.majorana.ORM_ACCESS.*;

import com.majorana.ORM.BaseMajoranaEntity;

import com.google.common.reflect.TypeToken;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Test;
import org.apache.commons.lang3.tuple.Pair;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.LinkedList;

import java.util.List;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;


import java.util.HashMap;
import java.util.Map;
import java.lang.reflect.Field;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import org.junit.runner.Runner;

import static org.junit.jupiter.api.Assertions.*;

import  org.junit.jupiter.api.Test;
import  org.junit.jupiter.api.BeforeEach;
import  org.junit.jupiter.api.AfterEach;


public class EntityFieldsTest {

    private static final com.majorana.Utils.MethodPrefixingLogger LOGGER = com.majorana.Utils.MethodPrefixingLoggerFactory.getLogger(EntityFieldsTest.class);

    private final static String PREFIX = "Distiller";

    private final static String DB_VERSION = "DBVersion";

    private final static String MAJOR = "Major";

    private final static String MINOR = "Minor";

    private final static String CHECK_DBVERSION = "CheckDbVersion";

    private static DbBean dbBean  = new DbBean();

    private static final String ENTITIES_DIR = "Distiller.entities";

    private static final String PACKAGE_TOP = "Distiller";

    private static final Boolean VERSBOSE = false;

    public EntityFieldsTest(){
//        dbBean = new com.majorana.ORM_ACCESS.DbBean();
//        try {
//            dbBean.connect();
//        } catch (Exception e){
//            LOGGER.error("could not connect to db");
//        }
    }

    @BeforeEach
    public void setup() throws Exception {
 //       dbBean = new com.majorana.ORM_ACCESS.DbBean();
 //       dbBean.connect();
    }

    @AfterEach
    public void tearDown() throws Exception {
    //    dbBean = new com.majorana.ORM_ACCESS.DbBean();
//        dbBean.close();
    }

    @Test
    public void listEntities() {
        SubClassFinder finder = new SubClassFinder();
        EntityFinder ef = new EntityFinder();
        //    Set<BaseDistillerEntity> entityTypes = finder.getSubclassInstances("Distiller", BaseDistillerEntity.class);
        Set<BaseDistillerEntity> entityTypes = ef.getEntities("Distiller");
        for (BaseDistillerEntity bde : entityTypes) {
            System.err.println(" ---- Entity ---");
            System.err.println(bde.getClass().getCanonicalName());
        }
    }


    @Test
    public void listField() {

          SubClassFinder finder = new SubClassFinder();
        EntityFinder ef = new EntityFinder();
    //    Set<BaseDistillerEntity> entityTypes = finder.getSubclassInstances("Distiller", BaseDistillerEntity.class);
        Set<BaseDistillerEntity> entityTypes = ef. getEntities("Distiller");
        for( BaseDistillerEntity bde : entityTypes) {
            System.err.println(" ---- ALL ---");
            System.err.println(bde.getClass().getCanonicalName());
            System.err.println(bde.getFields());
            LOGGER.warn(" ---- ");
            LOGGER.warn(bde.getClass().getCanonicalName());
            List<MajoranaRepositoryField> maflist = MajoranaAnnotationRepository.getRepositoryFields(bde.getClass());
            System.err.println(bde.getClass().getCanonicalName());
            LOGGER.warn(bde.getClass().getCanonicalName());
            if (VERSBOSE) {
                for (MajoranaRepositoryField maf : maflist) {
                    LOGGER.warn(maf.toString());
                }
            } else {
                System.err.println(" Field count " + maflist.size());
            }
        }
    }

    @Test
    public void listField_1_0() {
        SubClassFinder finder = new SubClassFinder<BaseDistillerEntity>(1,0);
     //   EntityFinder ef = new EntityFinder();
        Pair<Integer, Integer>  pa = finder.getHighestVersionInPackage("Distiller.entities");
        LOGGER.warn("Distiller.entities major="+pa.getLeft()+ "minor="+pa.getRight());
        Set<BaseDistillerEntity> entityTypes = finder.getSubclassInstancesInPackagePrefix( PACKAGE_TOP, BaseDistillerEntity.class);
        for( BaseDistillerEntity bde : entityTypes){
            System.err.println( " ----  V1 ----");
            List<MajoranaRepositoryField> maflist = MajoranaAnnotationRepository.getRepositoryFields(bde.getClass());
            System.err.println( bde.getClass().getCanonicalName());
            LOGGER.warn( bde.getClass().getCanonicalName());
            if (VERSBOSE){
                for (MajoranaRepositoryField maf : maflist) {
                    LOGGER.warn(maf.toString());
                }
            } else {
                System.err.println(" Field count "+maflist.size());
            }
            LOGGER.warn( " ---- ");
            LOGGER.warn( bde.getClass().getCanonicalName());
            LOGGER.warn( bde.getFields());
        }
    }

    @Test
    public void listField_2_0() {

        SubClassFinder finder = new SubClassFinder<BaseDistillerEntity>(2,0);
        Pair<Integer, Integer>  pa = finder.getHighestVersionInPackage("Distiller.extraEntities");
        LOGGER.warn("Distiller.entities major="+pa.getLeft()+ "minor="+pa.getRight());
        Set<BaseDistillerEntity> entityTypes = finder.getSubclassInstancesInPackagePrefix("Distiller.extraEntities", BaseDistillerEntity.class);
        for( BaseDistillerEntity bde : entityTypes){
            System.err.println( " ---- V2 -----");
            System.err.println( bde.getClass().getCanonicalName());
            LOGGER.warn( bde.getClass().getCanonicalName());
            List<MajoranaRepositoryField> maflist = MajoranaAnnotationRepository.getRepositoryFields(bde.getClass());
            System.err.println( bde.getClass().getCanonicalName());
            LOGGER.warn( bde.getClass().getCanonicalName());
            if (VERSBOSE){
                for (MajoranaRepositoryField maf : maflist) {
                    LOGGER.warn(maf.toString());
                }
            } else {
                System.err.println(" Field count "+maflist.size());
            }
            LOGGER.warn( " ---- ");
            LOGGER.warn( bde.getClass().getCanonicalName());
            LOGGER.warn( bde.getFields());
        }
    }

    @Test
    public void dbCrudRandomWithDbBean() throws Exception {
        dbBean = new com.majorana.ORM_ACCESS.DbBean();
        dbBean.connect();
        EntityFinder ef = new EntityFinder();
        Set<BaseDistillerEntity> entityTypes = ef. getEntities(ENTITIES_DIR);
  //      SubClassFinder finder = new SubClassFinder<BaseDistillerEntity>();
  //      Set<Class> entityTypes = finder.findSubclasses(BaseDistillerEntity.class);
        int okST = 0;
        List<String> goodST = new LinkedList<>();
        int failedST = 0;
        List<String> badST = new LinkedList<>();

        int okRead=0;
        List<String> goodReader = new LinkedList<>();
        int badREAD=0;
        List<String> badReader = new LinkedList<>();


        int okUP = 0;
        List<String> goodUP = new LinkedList<>();
        int failedUP = 0;
        List<String> badUP= new LinkedList<>();

        int okDEL = 0;
        List<String> goodDEL = new LinkedList<>();
        int failedDEL = 0;
        List<String> badDEL = new LinkedList<>();

        for (BaseDistillerEntity t : entityTypes) {
            Class et = t.getClass();
            // STore and Get
            String className = et.getCanonicalName();
            LOGGER.warn("----------");
            LOGGER.warn(className);
            Class<BaseDistillerEntity> cl = (Class<BaseDistillerEntity> ) et;
            try {
                RandomEntity<BaseDistillerEntity> rde = new RandomEntity<>(dbBean, cl);
                BaseDistillerEntity bse= rde.getValue();
                String table = bse.getTableName();
                com.majorana.ORM_ACCESS.MultiId id = dbBean.storeBean(rde.getValue());
                assertTrue(
                        id.hasAnyId(), "Storing " + cl + " failed Storing " + cl + " failed "
                );
                okST++;
                goodST.add(et.getCanonicalName());
                String paramNames[] = new String[1];
                paramNames[0] = "id";
                Object params[] = new Object[1];
                params[0] = id.getId();
                MajoranaRepositoryField idField = dbBean.getIdField(et);
                MajoranaAnnotationRepository  repo = dbBean.getRepo(et);
                int id1 = (Integer) MajoranaAnnotationRepository.invokeGetter(bse, idField.getGetter());
                assertEquals( id.getId(), id1, "Equals ids from multiId and stored ");
                BaseDistillerEntity tbde = (BaseDistillerEntity) dbBean.getBeanNP(et, rde.getValue().getTableName(),
                        "SELECT "+dbBean.getFields(cl)+" FROM "+table+" WHERE "+dbBean.getIdFieldName(cl)+"=:id ", paramNames, params);
                List<String> failReader = rde.diffForCreateFields(tbde);
                if(!failReader.isEmpty()){
                    badReader.add(et.getName());
                    badREAD++;
                      fail("Stored fields not equals for clazz "+cl.getCanonicalName()+" rbde "+rde.getValue()+"/n!=/n"+tbde+"\nFields error"+failReader);
                }    else {
                    okRead++;
                    goodReader.add(et.getName());
                }
                BaseDistillerEntity upCandidate = rde.getValue();
                try {
                    MajoranaAnnotationRepository.invokeSetter(upCandidate, id, idField.getSetter());
                } catch (Exception e){
                    LOGGER.error(" SET id failed ",e, idField.getName());
                }
                dbBean.updateBean( id , upCandidate);
                BaseDistillerEntity modBean = (BaseDistillerEntity) dbBean.getBeanNP(et, rde.getValue().getTableName(),
                       "SELECT "+ dbBean.getFields(et)+" FROM "+table+ " WHERE "+dbBean.getIdFieldName(cl)+"=:id ", paramNames, params);
                for (MajoranaRepositoryField field :  MajoranaAnnotationRepository.getRepositoryFields(et)) {
                    if (field.isId()) {
                        int id2 = (Integer) MajoranaAnnotationRepository.invokeGetter(modBean, field.getGetter());
                        assertEquals( id1, id2, " Update wrong " + et + " id ");
                    } else if (field.isUpdateable()) {
                        Object after = MajoranaAnnotationRepository.invokeGetter(modBean, field.getGetter());
                        Object before = MajoranaAnnotationRepository.invokeGetter(upCandidate, field.getGetter());
                        assertTrue(SQLFieldEquality.checkField(field.getName(), after, before));
//                        assertEquals( after, before, " Update wrong " + className + " not uodated - incorrect ");
                    } else {
                        Object after = MajoranaAnnotationRepository.invokeGetter(modBean, field.getGetter());
                        Object before = MajoranaAnnotationRepository.invokeGetter(tbde, field.getGetter());
                        assertTrue(SQLFieldEquality.checkField(field.getName(), after, before));
                        assertEquals( after, before, " Update wrong " + className + " was uodated - incorrect ");
                    }

                }
                okUP++;
                goodUP.add(et.getCanonicalName());
                dbBean.deleteBeanById(id, tbde);
                BaseDistillerEntity delBean = (BaseDistillerEntity) dbBean.getBeanNP(et, tbde.getTableName(),
                        "SELECT "+dbBean.getFields(et)+" FROM "+table+" WHERE "+dbBean.getIdFieldName(cl)+"=:id ", paramNames, params);
                assertTrue( delBean == null, " Bean " + className + " not deleted ");
                okDEL++;
                goodDEL.add(et.getCanonicalName());
            } catch (Exception e) {
                badDEL.add(et.getCanonicalName());
                LOGGER.error("Failed for type " + et, e);
                failedDEL++;
            }
            dbBean.close();

        }

        LOGGER.warn("DB tests STORE passed for " + okST + " classes " + goodST.stream().collect(Collectors.joining(", ")));
        LOGGER.warn("DB tests STORE failed for " + failedST + " classes " + badST.stream().collect(Collectors.joining(", ")));

        LOGGER.warn("DB tests passed UPDATE for " + okUP + " classes " + goodUP.stream().collect(Collectors.joining(", ")));
        LOGGER.warn("DB tests failed UPDATE for " + failedUP + " classes " + badUP.stream().collect(Collectors.joining(", ")));

        LOGGER.warn("DB tests passed DEL for " + okDEL + " classes " + goodDEL.stream().collect(Collectors.joining(", ")));
        LOGGER.warn("DB tests failed DEL for " + failedDEL + " classes " + badDEL.stream().collect(Collectors.joining(", ")));
    }

    @Test
    public void junkTest() throws Exception {
        Class et = Distiller.entities.JunkUser.class;

        DbBean dbBean = new DbBean();
        DbBeanGenericInterface face = dbBean.getTypedBean( et );
        RandomEntity<BaseDistillerEntity> rbde = new RandomEntity<>(dbBean, et);

        face.storeBean(rbde.getValue());
    }


    @Test
    public void dbCrudRandomWithTypedDbBean() throws Exception {
    //    SubClassFinder finder = new SubClassFinder<BaseDistillerEntity>();
    //    Set<Class> entityTypes = finder.findSubclasses(  BaseDistillerEntity.class);
        List<MajoranaRepositoryField> maflist = MajoranaAnnotationRepository.getRepositoryFields(Distiller.entities.JunkUser.class);
        EntityFinder ef = new EntityFinder();
        dbBean = new DbBean();
        dbBean.connect();
        Set<BaseDistillerEntity> entityTypes = ef. getEntities(ENTITIES_DIR);
        int okRead=0;
        List<String> goodREAD = new LinkedList<>();
        int failedRead=0;
        List<String> badREAD = new LinkedList<>();
        int ok =0;
        List<String> good = new LinkedList<>();
        int failed = 0;
        List<String> bad = new LinkedList<>();
        int okST = 0;
        List<String> goodST = new LinkedList();
        int failedST = 0;
        List<String> badST = new LinkedList();



        int okUP = 0;
        List<String> goodUP = new LinkedList();
        int failedUP = 0;
        List<String> badUP = new LinkedList();
        int okDEL = 0;
        List<String> goodDEL = new LinkedList();
        int failedDEL = 0;
        List<String> badDEL = new LinkedList();
        DbBean dbBean = new DbBean();
        for(BaseDistillerEntity bde : entityTypes) {
            // STore and Get
            Class et = bde.getClass();
            String className = et.getCanonicalName();
             DbBeanGenericInterface face = dbBean.getTypedBean( et );
            try {
                RandomEntity<BaseDistillerEntity> rbde = new RandomEntity<>(dbBean, et);
                com.majorana.ORM_ACCESS.MultiId id = face.storeBean(rbde.getValue());
                String table = bde.getTableName();
                if(!id.hasAnyId()){
                    LOGGER.error("Storing " + et + " failed");
                        failedST++;
                        badST.add(et.getCanonicalName());
                        continue;
                    }
                String paramNames[] = new String[1];
                Object params[] = new Object[1];
                params[0] = id.getId();
                MajoranaRepositoryField idField = face.getIdField();
                if (idField==null){
                    LOGGER.error(className+" no id field found");
                    failedST++;
                    badST.add(et.getCanonicalName());
                    continue;
              } else {
                okST++;
                goodST.add(et.getCanonicalName());
            }
                MajoranaAnnotationRepository rep = face.getRepo();
                //MajoranaRepositoryField idField = rep.getIdField();
                paramNames[0] = idField.getDbColumn();

                int id1 = (Integer) MajoranaAnnotationRepository.invokeGetter(rbde.getValue(), idField.getGetter());
                assertEquals( id.getId(), id1, "Equals ids from multiId and stored ");

                String sql = "SELECT "+face.getFields()+" FROM "+bde.getTableName()+" WHERE "+face.getIdField().getDbColumn()+"=:"+paramNames[0];

                LOGGER.warn("Reading '"+sql+"'"+ params[0]);

                BaseDistillerEntity tbde = (BaseDistillerEntity) face.getBeanNP(sql, paramNames, params);

                List<String> neq = rbde.diffForCreateFields(tbde);

                if (!neq.isEmpty()){
                        LOGGER.error(className+" Stored fields not equals "+ neq.stream().collect(Collectors.joining(", ")));
                    failedRead++;
                    badREAD.add(et.getCanonicalName());
                    continue;
                } else {
                    okRead++;
                    goodREAD.add(et.getCanonicalName());
                }
                RandomEntity<BaseDistillerEntity> rbde1 = new RandomEntity<>(dbBean, et);
                BaseDistillerEntity upCandidate = rbde1.getValue();
                MajoranaAnnotationRepository.invokeSetter(upCandidate, id.getId(), idField.getSetter());
                face.updateBean(id, upCandidate);

                face.updateAltIds(id, upCandidate);

                BaseDistillerEntity modBean = face.getBeanNP(
                                "SELECT "+ dbBean.getFields(et)+" FROM "+table+ " WHERE "+face.getIdFieldName()+"=:"+
                                        idField.getDbColumn(), paramNames, params);
                boolean upGood=true;
                for (MajoranaRepositoryField field : rep.getRepositoryFields(et)) {
                    if (field.isId() && field.getValueType()!=UUID.class) {
                        int id2 = (Integer) MajoranaAnnotationRepository.invokeGetter(modBean, field.getGetter());
                        LOGGER.warn(id1 + " != " + id2 + " Update wrong " + et + " id");
                    } else if (field.isAltId()) {
                            int id2 = (Integer) MajoranaAnnotationRepository.invokeGetter(modBean, field.getGetter());
                            LOGGER.warn(id1+" != "+id2+ " Update Alt Id wrong " + et + " id");

                    } else if (field.isUpdateable()) {
                        Object after = MajoranaAnnotationRepository.invokeGetter(modBean, field.getGetter());
                        Object before = MajoranaAnnotationRepository.invokeGetter(upCandidate, field.getGetter());
                        if (!after.equals(before)) {
                            LOGGER.warn(before + " " + after + " Update wrong " + className + " not uodated - incorrect ");
                            upGood = false;
                        }
//                            badUp.add(field.getName());
                        // badUp.add(field.getName());
                    } else {
                        Object after = MajoranaAnnotationRepository.invokeGetter(modBean, field.getGetter());
                        Object before = MajoranaAnnotationRepository.invokeGetter(tbde, field.getGetter());
                        if (!after.equals(before)) {
                            LOGGER.warn(before + " " + after + " Update wrong " + className + " was uodated - incorrect ");
                            upGood = false;
                            //       badUp.add(field.getName());
                        }
                    }
                    if (!upGood) {
                        badUP.add(className);
                        failedUP++;
                    } else {
                        goodUP.add(className);
                        okUP++;
                    }
                }

                //               ok++;
//                good.add(et.getCanonicalName());
                face.deleteBeanById(id);
                String sql_del = "SELECT "+face.getFields()+" FROM "+bde.getTableName()+" WHERE "+idField.getDbColumn()+"="+id.getId();
                List<BaseDistillerEntity> delBeans = face.getBeansNP(sql_del, new String[0], new Object[0]);
                if (delBeans.size()!=0){
                    LOGGER.warn(" Bean " + className + " not deleted ");
                    badDEL.add(className);
                    failedDEL++;
                } else {
                    goodDEL.add(et.getCanonicalName());
                    okDEL++;
                }
            } catch (Exception e) {
                bad.add(et.getCanonicalName());
                LOGGER.error("Failed for type " + et, e);
                failed++;
            }
        }
        LOGGER.warn("DB tests passed for "+ok+" classes "+good.stream().collect(Collectors.joining(", ")));
        LOGGER.warn("DB tests failed for "+failed+" classes "+bad.stream().collect(Collectors.joining(", ")));
        LOGGER.warn("DB tests STORE passed for " + okST + " classes " + goodST.stream().collect(Collectors.joining(", ")));
        LOGGER.warn("DB tests STORE failed for " + failedST + " classes " + badST.stream().collect(Collectors.joining(", ")));

        LOGGER.warn("DB tests STORE READ passed for " + okRead + " classes " + goodREAD.stream().collect(Collectors.joining(", ")));
        LOGGER.warn("DB tests STORE READ failed for " + failedRead + " classes " + badREAD.stream().collect(Collectors.joining(", ")));

        LOGGER.warn("DB tests passed UPDATE for " + okUP + " classes " + goodUP.stream().collect(Collectors.joining(", ")));
        LOGGER.warn("DB tests failed UPDATE for " + failedUP + " classes " + badUP.stream().collect(Collectors.joining(", ")));

        LOGGER.warn("DB tests passed DEL for " + okDEL + " classes " + goodDEL.stream().collect(Collectors.joining(", ")));
        LOGGER.warn("DB tests failed DEL for " + failedDEL + " classes " + badDEL.stream().collect(Collectors.joining(", ")));

        LOGGER.warn("DB tests passed UPDATE for " + okUP + " classes " + goodUP.stream().collect(Collectors.joining(", ")));
        LOGGER.warn("DB tests failed UPDATE for " + failedUP + " classes " + badUP.stream().collect(Collectors.joining(", ")));

        LOGGER.warn("DB tests passed DEL for " + okDEL + " classes " + goodDEL.stream().collect(Collectors.joining(", ")));
        LOGGER.warn("DB tests failed DEL for " + failedDEL + " classes " + badDEL.stream().collect(Collectors.joining(", ")));
    }




}
