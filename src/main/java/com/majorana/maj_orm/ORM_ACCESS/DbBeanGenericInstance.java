
package com.majorana.maj_orm.ORM_ACCESS;

//import com.datastax.driver.core.Session;
import com.majorana.maj_orm.DBs.*;
import com.majorana.maj_orm.ORM.MajoranaAnnotationRepository;
import com.majorana.maj_orm.ORM.MajoranaDBConnectionFactory;
import com.majorana.maj_orm.ORM.MajoranaRepositoryField;
import com.majorana.maj_orm.Utils.MethodPrefixingLoggerFactory;
import com.majorana.maj_orm.ORM.BaseMajoranaEntity;
import com.majorana.maj_orm.DBs.CassandraState;
import com.majorana.maj_orm.DBs.DatabaseVariant;
import com.majorana.maj_orm.DBs.MajDataSourceName;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.KeyHolder;
import java.sql.Timestamp;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.lang.reflect.ParameterizedType;
import java.sql.SQLException;
import java.util.Date;
import java.util.*;


import static org.mockito.Mockito.mock;

;
;

public class DbBeanGenericInstance<T extends BaseMajoranaEntity> implements DbBeanGenericInterface<T> {

  private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(DbBeanGenericInstance.class);
  private Connection dbCon;

  private HikariDataSource ds;
// Cassandra Session
//  private CqlSession cs;

  private MajoranaDBConnectionFactory factory;

  private JdbcTemplate jdbcTemplate;

  private NamedParameterJdbcTemplate namedTemplate;

  //private CassandraTemplate cassandraTemplate;

//  private CqlTemplate cqlTemplate;

  private CassandraState cassandraState;

  private CassandraState cassandraIsMain;

  private CassandraState cassandraDbIsPresent;

  private MajoranaAnnotationRepository classRepo;

//  private CassandraTemplate  mockCass = null;

  private boolean isCass;


  private static DbBeanGenericInstance singletonLazy;

  private MajDataSourceName mainDsn = null;
  private  MajDataSourceName cassDsn = null;
  private  MajDataSourceName jdbcDsn = null;
  private  int minMinor  = 0;
  private  int minMajor = 0;

  private Class<T> clazz = null;

  private T[] clazzArray = null;

  private String table;

    public static class BeanTyper<T extends BaseMajoranaEntity> {

        public  synchronized <T  extends BaseMajoranaEntity> DbBeanGenericInterface<? extends BaseMajoranaEntity> getTypedBean(DbBean dbBean, DbBeanInstance mainBean, List<T> S, Class S1){
            ParameterizedType paramType;
            paramType = (ParameterizedType) S.getClass().getGenericInterfaces()[0];
            Class S2 = paramType.getActualTypeArguments()[0].getClass();
            if (dbBean.getTypedBeanFromStore(S1)!=null){
                return dbBean.getTypedBeanFromStore(S1);
            }
            DbBeanGenericInstance<T> dbi = new DbBeanGenericInstance<T>( dbBean, S1);
            dbi.clazz= S1;
            dbi.jdbcDsn = dbBean.getMainBean().getJdbcDsn();
//            dbi.cassDsn = dbBean.getMainBean().getCassDsn();
            dbi.mainDsn = dbBean.getMainBean().getMainDsn();
        //    dbi.cassandraTemplate = mainBean.getCassandraTemplate();
        //    dbi.cqlTemplate = mainBean.getCqlTemplate();
            dbi.jdbcTemplate = mainBean.getJdbcTemplate();
            dbi.namedTemplate = mainBean.getNamedTemplate();
            dbi.factory = mainBean.getFactory();
       //     dbi.cs= mainBean.getCs();
            dbi.ds = mainBean.getDs();
            try {
                dbi.clazzArray = (T[]) Array.newInstance(S1, 0);
            } catch (Exception e){
                LOGGER.warn("Exception creating array ",e);
            }
            dbBean.putTypedBeanToStore(S1, dbi);
            return dbi;

        }

    }


  protected DbBeanGenericInstance(DbBean dbBean, Class<T> clazz){
      this.clazz = clazz;
      this.factory = dbBean.getFactory();
    //  if (!dbBean.getFactory().getMainCassandraTemplate().isPresent()) {
    //      cassandraTemplate = mock(CassandraTemplate.class);
    //      cs = mock(CqlSession.class);
    //  } else {
     //     cassandraTemplate = factory.getMainCassandraTemplate().orElse(null);
    //      cqlTemplate = (CqlTemplate) cassandraTemplate.getCqlOperations();
    //      if (cqlTemplate!=null) {
    //          cs = cqlTemplate.getSession();
    //      } else {
    //          cassandraTemplate = mock(CassandraTemplate.class);
    //          cs = mock(CqlSession.class);
    //      }
    //  }
      factory = dbBean.getFactory();
      jdbcTemplate = factory. getMainJdbcTemplate().orElse(null);
      namedTemplate  = factory.getMainNamedParamJdbcTemplate().orElse(null);

      cassandraIsMain = new CassandraState(factory.getMainVariant() == DatabaseVariant.CASSANDRA);
      if (jdbcTemplate==null){
          LOGGER.warn("Jdbc DB connnection not found");
      }
      classRepo = dbBean.getRepo(clazz);
      try {
        if (clazz.newInstance() instanceof BaseMajoranaEntity) {

            table = ((BaseMajoranaEntity) clazz.newInstance()).getTableName();
            clazzArray = (T[]) new LinkedList<T>().toArray();
        }
      } catch (Exception e){
              LOGGER.warn(" "+clazz+" does not subclass BaseMajoranaEntity");

      }
  }

    @Override
    public MajoranaAnnotationRepository<T> getRepo() {
        return classRepo;
    }

    public MajoranaRepositoryField getIdField(){
      return classRepo.getIdField();
    }

    public T getBean( String sql1, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo();

        String sql = mj. getReadStringSelectClause( table, sql1);
//        String sql = mj.getR

        if (isCass){
            try {
            //    PreparedStatement pres = cs.prepare(sql);
            //    BoundStatement bounds = pres.bind(params);
            //    return (T) cassandraTemplate.selectOne(bounds, clazz );
                return null;
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                return (T) jdbcTemplate.queryForObject(sql, params, mj.getMapper());
            } catch (Exception e){
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }


    public T[] getBeansArray( String sql, String[] paramNames, Object[] params) throws SQLException {
        List<T> obj = getBeans(sql, paramNames, params);
        return obj.toArray(clazzArray);
    }


    public T[] getBeansArrayNP( String sql, String[] paramNames, Object[] params) throws SQLException {
        List<T> obj = getBeansNP( sql, paramNames, params);
        return obj.toArray(clazzArray);
    }

    public T[] getBeansArrayNPWithSelectClause( String sql, String[] paramNames, Object[] params) throws SQLException {
        List<T> obj = getBeansNPWithSelectClause( sql, paramNames, params);
        return obj.toArray(clazzArray);
    }


    public T getBeanNP( String sql, String[] paramNames, Object[] params) throws SQLException {
        if (isCass){
            try {
//                PreparedStatement pres = cs.prepare(sql);
//                BoundStatement bounds = pres.bind(params);
//                return (T) cassandraTemplate.selectOne(bounds, clazz );

            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
            return null;
        } else {
            Map<String, Object> paraMap = getParamMap(paramNames, params);
            try {
                NamedParameterJdbcTemplate npj = factory.getMainNamedParamJdbcTemplate().orElse(null);
                if (npj==null){ return null; }
                return (T) npj.queryForObject(sql, paraMap, getRepo().getMapper());
            } catch (Exception e){
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }

    public T getBeanNPWithSelectClause( String sql, String[] paramNames, Object[] params) throws SQLException {
        if (isCass){
            try {
      //          PreparedStatement pres = cs.prepare(sql);
      //          BoundStatement bounds = pres.bind(params);
      //          return (T) cassandraTemplate.selectOne(bounds, clazz );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
            return null;
        } else {
            Map<String, Object> paraMap = getParamMap(paramNames, params);
            try {
                NamedParameterJdbcTemplate npj = factory.getMainNamedParamJdbcTemplate().orElse(null);
                if (npj==null){ return null; }
                return (T) npj.queryForObject(sql, paraMap, getRepo().getMapper());
            } catch (Exception e){
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }


    public Map<String, Object> getParamMap(String named[], Object params[]){
        HashMap<String, Object> ret = new HashMap<>();
        int len= Math.min(named.length, params.length);
        for(int i=0; i<len; i++){
            ret.put(named[i], params[i]);
        }
        return ret;
    }

    @Override
    public String getFields() {
        return getRepo().getSqlFieldString();
    }

    @Override
    public String getFields(String pre) {
        return getRepo().getSqlFieldStringWithPrefix(pre);
    }

    @Override
    public String getIdFieldName(){
        return getRepo().getIdField().getDbColumn();
    }


    public List<T> getBeans(String sql1,  String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo();




        String sql = mj. getReadStringNPSelectClause( table, sql1, paramNames, params);
//        String sql = mj.getR

        if (isCass){
            try {
      //         PreparedStatement pres = cs.prepare(sql);
      //         BoundStatement bounds = pres.bind(params);
      //          return (List<T>) cassandraTemplate.select(bounds, clazz );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
            return null;
        } else {
            try {
//                MapSqlParameterSource src = new MapSqlParameterSource(getParamMap(paramNames, params));
                return (List<T>) jdbcTemplate.query(sql, params, mj.getMapper());
            } catch (Exception e){
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }

    public int deleteBeans( String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo();



        String sql = mj. getDeleteString( table,  paramNames, params);
//        String sql = mj.getR

        if (isCass){
            try {
   //            PreparedStatement pres = cs.prepare(sql);
   //             BoundStatement bounds = pres.bind(params);
   //            com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute(bounds);
   //             return rs.all().size();
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
            return 0;
        } else {
            try {
                return jdbcTemplate.query(sql, mj.getMapper()).size();
            } catch (Exception e){
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }

    public int updateAltIds(MultiId mid, T bde) throws SQLException {

        if  (bde==null){
            LOGGER.error("Null Data sent to bean store");
            return 0;
        }
        Class beanClass = bde.getClass();

        MajoranaAnnotationRepository mj = getRepo();

        String idCol = mj.getIdField().getDbColumn();
        String uuidCol = mj.getKeyUuid();

        if (!mid.hasAnyId()){
            LOGGER.warn("Not updating "+bde.getTableName()+ "  no id given");
            return 0;
        }

        String where = mid.hasId() ? idCol+"="+mid.getId() : "";
        where = where + (mid.hasBoth() ? " AND " : "");
        where = where + ((mid.hasUuid() ? uuidCol = "'"+mid.getUUID().toString()+"'"
                : ""));

        String sql = "UPDATE "+bde.getTableName()+ " "+mj. getUpAltIdStringNP( bde)
                + " WHERE "+where;
//        String sql = mj.getR

        KeyHolder kh = new org.springframework.jdbc.support.GeneratedKeyHolder();

        if (isCass){
            try {
      //          PreparedStatement pres = cs.prepare(sql);
                //org.springframework.data.cassandra.core.query.Update bounds =
        //        org.springframework.data.cassandra.core.query.Update.empty();
                //Map<String, Object> data = mj. getParameterMapWithDeletedAt(cassDsn, bde);
                //for(Map.Entry<String, Object> en : data.entrySet()){
                //    bounds = bounds.set( en.getKey(), en.getValue());
                //}
                //        org.springframework.data.cassandra.core.query.Update.addAll( mj.getSqlParameterMapWithDeletedAt(cassDsn,bde))

//                PreparedStatement preparedStatement = cql.prepare("insert into plans (user_id, plan) values (?, ? )");
          //      BoundStatement upStatement = pres.bind(mj.getParameterMapWithDeletedAt(cassDsn, bde) );
                //CqlTemplate ct = cs.
         //       com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute
        //                (upStatement);
        //        Row row  = rs.one();
        //        if (row==null){ return 0; }
                return 1;

                //pres.bind( mj. getSqlParameterSourceWithDeletedAt(bde)); //params);
                // return cassandraTemplate.update(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                Map<String, Object> upMap = mj.getParameterMapWithDeletedAt(jdbcDsn, bde);
                int ch = namedTemplate.update(sql, new MapSqlParameterSource(upMap),kh);
                return ch;
            } catch (Exception e){
                LOGGER.warn("Error Update sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }


    public MultiId storeBean( T bde) throws SQLException {

      if  (bde==null){
          LOGGER.error("Null Data sent to bean store");
          return new MultiId(0);
      }

        MajoranaAnnotationRepository mj = getRepo();

        String sql = "INSERT INTO "+ table+" "+mj. getCreateStringNP( bde);
//        String sql = mj.getR

        KeyHolder kh = new org.springframework.jdbc.support.GeneratedKeyHolder();

        if (isCass){
            try {
        //       PreparedStatement pres = cs.prepare(sql);
                //org.springframework.data.cassandra.core.query.Update bounds =
                        org.springframework.data.cassandra.core.query.Update.empty();
                //Map<String, Object> data = mj. getParameterMapWithDeletedAt(cassDsn, bde);
                //for(Map.Entry<String, Object> en : data.entrySet()){
                //    bounds = bound\s.set( en.getKey(), en.getValue());
                //}
                //        org.springframework.data.cassandra.core.query.Update.addAll( mj.getSqlParameterMapWithDeletedAt(cassDsn,bde))

//                PreparedStatement preparedStatement = cql.prepare("insert into plans (user_id, plan) values (?, ? )");
          //      BoundStatement insertStatement = pres.bind(mj.getParameterMapWithDeletedAt(cassDsn, bde) );
                
                //CqlTemplate ct = cs.
            //    com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute(insertStatement);
           //    Row row  = rs.one();
             //   if (row==null){ return new MultiId(); }
                return new MultiId(getUUID(mj.getKeyUuid()));
                
                //pres.bind( mj. getSqlParameterSourceWithDeletedAt(bde)); //params);
                // return cassandraTemplate.update(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                int ch =factory.getNamedParameterJdbcTemplate(jdbcDsn).orElseThrow(
                        RuntimeException::new
                ).update(sql, mj. getSqlParameterSourceWithDeletedAt(jdbcDsn, bde),kh);
                if (ch!=0){
                    Number n = kh.getKey();
                    if (n!=null){
                        return new MultiId(n.intValue());
                    }
                }
                return new MultiId();
            } catch (Exception e){
                LOGGER.warn("Error Update sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }

    public MultiId updateBean( MultiId mid,  T bde) throws SQLException {

        Class beanClass = bde.getClass();

        MajoranaAnnotationRepository mj = getRepo();

        String idCol = mj.getIdField().getDbColumn();
        String uuidCol = mj.getKeyUuid();
        
        if (!mid.hasAnyId()){
            LOGGER.warn("Not updating "+bde.getTableName()+ "  no id given");
            return mid;
        }
        
        String where = mid.hasId() ? idCol+"="+mid.getId() : "";
        where = where + (mid.hasBoth() ? " AND " : "");
        where = where + ((mid.hasUuid() ? uuidCol = "'"+mid.getUUID().toString()+"'"
                : ""));
        
        String sql = "UPDATE "+bde.getTableName()+ " "+mj. getUpdateStringNPSansWhere( bde)
                + " WHERE "+where;
//        String sql = mj.getR

        KeyHolder kh = new org.springframework.jdbc.support.GeneratedKeyHolder();

        if (isCass){
            try {
     //          PreparedStatement pres = cs.prepare(sql);
                //org.springframework.data.cassandra.core.query.Update bounds =
                org.springframework.data.cassandra.core.query.Update.empty();
                //Map<String, Object> data = mj. getParameterMapWithDeletedAt(cassDsn, bde);
                //for(Map.Entry<String, Object> en : data.entrySet()){
                //    bounds = bounds.set( en.getKey(), en.getValue());
                //}
                //        org.springframework.data.cassandra.core.query.Update.addAll( mj.getSqlParameterMapWithDeletedAt(cassDsn,bde))

//                PreparedStatement preparedStatement = cql.prepare("insert into plans (user_id, plan) values (?, ? )");
       //         BoundStatement upStatement = pres.bind(mj.getParameterMapWithDeletedAt(cassDsn, bde) );
                //CqlTemplate ct = cs.
        //        com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute
         //              (upStatement);
         //      Row row  = rs.one();
           //     if (row==null){ return new MultiId(); }
                return new MultiId(getUUID(mj.getKeyUuid()));

                //pres.bind( mj. getSqlParameterSourceWithDeletedAt(bde)); //params);
                // return cassandraTemplate.update(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                Map<String, Object> upMap = mj.getParameterMapWithDeletedAt(jdbcDsn, bde);
                int ch = namedTemplate.update(sql, new MapSqlParameterSource(upMap));
                if (ch!=0){
                    Number n = kh.getKey();
                    if (n!=null){
                        return new MultiId(n.intValue());
                    }
                }
                return new MultiId();
            } catch (Exception e){
                LOGGER.warn("Error Update sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }

    private UUID getUUID(String s){
      return UUID.fromString(s);
    }

    public MultiId deleteBeanById( MultiId mid) throws SQLException,InstantiationException, IllegalAccessException {


        MajoranaAnnotationRepository mj = getRepo();

        String idCol = mj.getIdField().getDbColumn();
        String uuidCol = mj.getKeyUuid();

        if (!mid.hasAnyId()){
            LOGGER.warn("Not updating "+table+ "  no id given");
            return mid;
        }



        String where = mid.hasId() ? idCol+"="+mid.getId() : "";
        where = where + (mid.hasBoth() ? " AND " : "");
        where = where + (mid.hasUuid() ? uuidCol = "'"+mid.getUUID().toString()+"'"
                : "");

        String sql = "DELETE from "+table
                + " WHERE "+where;
//        String sql = mj.getR

//        KeyHolder kh = new org.springframework.jdbc.support.GeneratedKeyHolder();

        if (isCass){
            try {
       //         PreparedStatement pres = cs.prepare(sql);
                //org.springframework.data.cassandra.core.query.Update bounds =
         //       org.springframework.data.cassandra.core.query.Update.empty();
                //Map<String, Object> data = mj. getParameterMapWithDeletedAt(cassDsn, bde);
                //for(Map.Entry<String, Object> en : data.entrySet()){
                //    bounds = bounds.set( en.getKey(), en.getValue());
                //}
                //        org.springframework.data.cassandra.core.query.Update.addAll( mj.getSqlParameterMapWithDeletedAt(cassDsn,bde))
                T bde  = (T) clazz.newInstance();
                bde.setId(mid.getId());
                bde.setUuid(mid.getUUID());
//                PreparedStatement prepar;edStatement = cql.prepare("insert into plans (user_id, plan) values (?, ? )");
          //      BoundStatement upStatement = pres.bind(mj.getParameterMapWithDeletedAt(cassDsn, bde) );
                //CqlTemplate ct = cs.
            //    com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute(upStatement);
            //   Row row  = rs.one();
        //        if (row==null){ return new MultiId(); }
                return new MultiId(mid.getUUID());

                //pres.bind( mj. getSqlParameterSourceWithDeletedAt(bde)); //params);
                // return cassandraTemplate.update(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                T bde = (T) clazz.newInstance();
                bde.setId(mid.getId());
                bde.setUuid(mid.getUUID());

                Map<String, Object> upMap = mj.getParameterMapWithDeletedAt(jdbcDsn, bde);
                int ch = namedTemplate.update(sql, new MapSqlParameterSource(upMap));
                if (ch!=0){
                        return new MultiId(mid.getId());

                }
                return new MultiId();
            } catch (Exception e){
                LOGGER.warn("Error Update sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }

    public MultiId deleteBeanByParams( MultiId mid,  BaseMajoranaEntity bde) throws SQLException {

        Class beanClass = bde.getClass();

        MajoranaAnnotationRepository mj = getRepo();

        String idCol = mj.getKeyId();
        String uuidCol = mj.getKeyUuid();

        if (!mid.hasAnyId()){
            LOGGER.warn("Not updating "+bde.getTableName()+ "  no id given");
            return mid;
        }

        String where = mid.hasId() ? idCol+"="+mid.getId() : "";
        where = where + (mid.hasBoth() ? " AND " : "");
        where = where + (mid.hasUuid() ? uuidCol = "'"+mid.getUUID().toString()+"'"
                : "");

        String sql = "DELETE from "+bde.getTableName()
                + " WHERE "+where;
//        String sql = mj.getR

//        KeyHolder kh = new org.springframework.jdbc.support.GeneratedKeyHolder();

        if (isCass){
            try {
    //           PreparedStatement pres = cs.prepare(sql);
                //org.springframework.data.cassandra.core.query.Update bounds =
        //        org.springframework.data.cassandra.core.query.Update.empty();
                //Map<String, Object> data = mj. getParameterMapWithDeletedAt(cassDsn, bde);
                //for(Map.Entry<String, Object> en : data.entrySet()){
                //    bounds = bounds.set( en.getKey(), en.getValue());
                //}
                //        org.springframework.data.cassandra.core.query.Update.addAll( mj.getSqlParameterMapWithDeletedAt(cassDsn,bde))

//                PreparedStatement preparedStatement = cql.prepare("insert into plans (user_id, plan) values (?, ? )");
      //          BoundStatement upStatement = pres.bind(mj.getParameterMapWithDeletedAt(cassDsn, bde) );
                //CqlTemplate ct = cs.
          //      com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute(upStatement);
          //     Row row  = rs.one();
          //      if (row==null){ return new MultiId(); }
                return new MultiId(mid.getUUID());

                //pres.bind( mj. getSqlParameterSourceWithDeletedAt(bde)); //params);
                // return cassandraTemplate.update(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                int ch = jdbcTemplate.update(sql, mj. getSqlParameterSourceWithDeletedAt(jdbcDsn,
                        bde));
                if (ch!=0){
                    return new MultiId(mid.getId());

                }
                return new MultiId();
            } catch (Exception e){
                LOGGER.warn("Error Update sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }


    public Object getBean( Class beanClass, String sql, Object... params) throws SQLException {

    MajoranaAnnotationRepository mj = getRepo();

    if (isCass){
       try {
  //        PreparedStatement pres = cs.prepare(sql);
  //         BoundStatement bounds = pres.bind(params);
  //          return cassandraTemplate.selectOne(bounds, beanClass );
        } catch (Exception e){
          LOGGER.warn("Error Executing cql in cassandra "+sql,e);
          throw e;
        }
       return null;
  } else {
        try {
            return jdbcTemplate.queryForObject(sql, mj.getMapper());
        } catch (Exception e){
            LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
            throw e;
        }
    }

  }

    public T[] getBeansArray(  String sql, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo();

        if (isCass){
            try {
     //          PreparedStatement pres = cs.prepare(sql);
     //           BoundStatement bounds = pres.bind(params);
     //           return (T[]) cassandraTemplate.select(bounds, clazz ).toArray();
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
            return null;
        } else {
            try {
                return (T[]) jdbcTemplate.query(sql, params, mj.getMapper()).toArray();
            } catch (Exception e){
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }


    public List<T> getBeansNP(  String sql, String[] paramNames, Object[] params) {
        MajoranaAnnotationRepository mj = getRepo();
        if (isCass){
            try {
   //             PreparedStatement pres = cs.prepare(sql);
   //             BoundStatement bounds = pres.bind(params);
  //              return cassandraTemplate.select(bounds, clazz );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
            return null;
        } else {
            try {
                Map<String, Object> paraMap = getParamMap(paramNames, params);
                MapSqlParameterSource src = new MapSqlParameterSource(paraMap);
                return namedTemplate.query(sql, src, mj.getMapper());
            } catch (Exception e){
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }
    }

    public List<T> getBeansNPWithSelectClause(  String sql1, String[] paramNames, Object[] params) {
        MajoranaAnnotationRepository mj = getRepo();
        String sql = mj.getReadStringNPSelectClause( table, sql1, paramNames, params);
        if (isCass){
            try {
    //            PreparedStatement pres = cs.prepare(sql);
    //            BoundStatement bounds = pres.bind(params);
    //            return cassandraTemplate.select(bounds, clazz );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
            return null;
        } else {
            try {
                Map<String, Object> paraMap = getParamMap(paramNames, params);
                MapSqlParameterSource src = new MapSqlParameterSource(paraMap);
                return namedTemplate.query(sql, src, mj.getMapper());
            } catch (Exception e){
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }
    }


    protected class FloatClass {
        private float f;

        public float getF() {
            return f;
        }

        public void setF(float f) {
            this.f = f;
        }
    }
    
    public List<T> getBeans( String sql, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo();
        if (isCass){
            try {
//               PreparedStatement pres = cs.prepare(sql);
//                BoundStatement bounds = pres.bind(params);
//                return (List<T>) cassandraTemplate.select(bounds, clazz );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
            return null;
        } else {
            try {       

                return (List<T>) jdbcTemplate.query(sql, mj.getMapper());
            } catch (Exception e){
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

  }

    public static int mapParams(java.sql.PreparedStatement ps, Object[] args) throws SQLException {
        int i = 1;
        for (Object arg : args) {
            if (arg instanceof Date) {
                ps.setTimestamp(i++, new Timestamp(((Date) arg).getTime()));
            } else if (arg instanceof Integer) {
                ps.setInt(i++, (Integer) arg);
            } else if (arg instanceof Long) {
                ps.setLong(i++, (Long) arg);
            } else if (arg instanceof Double) {
                ps.setDouble(i++, (Double) arg);
            } else if (arg instanceof Float) {
                ps.setFloat(i++, (Float) arg);
            } else {
                ps.setString(i++, (String) arg);
            }
        }
    return i;
}



}
