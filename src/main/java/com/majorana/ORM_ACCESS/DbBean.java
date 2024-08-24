
package com.majorana.ORM_ACCESS;


import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.oss.driver.api.core.CqlSession;
import com.majorana.DBs.*;
import com.majorana.ORM.BaseMajoranaEntity;
import com.majorana.ORM.CassMockResultSet;
import com.majorana.ORM.MajoranaAnnotationRepository;
import com.majorana.ORM.MajoranaDBConnectionFactory;
import com.majorana.Utils.MethodPrefixingLoggerFactory;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.rowset.serial.SerialBlob;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

public class DbBean {

  String dbURL = "jdbc:mysql://localhost:3306/distiller";
  String dbDriver = "com.mysql.jdbc.Driver";
  String dbUser = "";
  String dbPass = "";

  private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(DbBean.class);
  private Connection dbCon;

  private HikariDataSource ds;
// Cassandra Session
  private Session cs;

  private MajoranaDBConnectionFactory factory;

  private JdbcTemplate jdbcTemplate;

  private NamedParameterJdbcTemplate namedTemplate;

  private CassandraTemplate cassandraTemplate;

  private CqlTemplate cqlTemplate;

  private DBEnvSetup envSetup ;

  private CassandraState cassandraState;

  private CassandraState cassandraIsMain;

  private CassandraState cassandraDbIsPresent;

  private Map<Class, MajoranaAnnotationRepository> classDBmap = new HashMap<>();

  private CassandraTemplate  mockCass = null;

  private boolean isCass;

  private EntityFinder entityFinder;

  private static DbBean singletonLazy;

  private MajDatasourceName mainDsn = null;
  private  MajDatasourceName cassDsn = null;
  private  MajDatasourceName jdbcDsn = null;
  private  int minMinor  = 0;
  private  int minMajor = 0;

  private DbBean(Map<String, String> addMap){
      cassandraState = new CassandraState(false);
      entityFinder = new EntityFinder();
      envSetup = new DBEnvSetup(cassandraState, addMap);
      factory = new MajoranaDBConnectionFactory(envSetup, new CassandraState(true));
      if (factory.getMainCassandraTemplate()==null){
          cassandraTemplate = mock(CassandraTemplate.class);
      }
      jdbcTemplate = factory. getMainJdbcTemplate().orElse(null);
      namedTemplate  = factory.getMainNamedParamJdbcTemplate().orElse(null);
      cassandraTemplate = factory.getMainCassandraTemplate().orElse(null);
      CqlTemplate cqlTemplate = (CqlTemplate) cassandraTemplate.getCqlOperations();
      cs = cqlTemplate.getSession();

      cassandraIsMain = new CassandraState(factory.getMainVariant() == DatabaseVariant.CASSANDRA);
      if (jdbcTemplate==null){
          LOGGER.warn("Jdbc DB coD/Nnnection "+envSetup. getMainDBName()+" not found");
      }
  }

    private DbBean(){
        cassandraState = new CassandraState(false);
        entityFinder = new EntityFinder();
        envSetup = new DBEnvSetup(cassandraState, new HashMap<>());
        factory = new MajoranaDBConnectionFactory(envSetup, new CassandraState(true));
        jdbcTemplate = factory. getMainJdbcTemplate().orElse(null);
        cassandraTemplate = factory.getMainCassandraTemplate().orElse(null);
        cassandraIsMain = new CassandraState(factory.getMainVariant() == DatabaseVariant.CASSANDRA);
        if (jdbcTemplate==null){
            LOGGER.warn("Jdbc DB coDNnnection "+envSetup. getMainDBName()+" not found");
        }
    }

    public PreparedStatement  getCassPreparedStatement(String sql){
        PreparedStatement preparedStatement = cqlTemplate.getSession().prepare(sql);
        return preparedStatement;
    }

  public void preloadEntities(){
      entityFinder.setAllowedVersions(envSetup.getDbVersionMajor(), envSetup.getDbVersionMinor());
      entityFinder.populateEntity(envSetup.isCheckDbVersion());
      List<String> pack = entityFinder.getPackages();
      for(String pak : pack){
          List<Pair<String, Class>> validEntries  = entityFinder.getValidEntities();
          for(Pair<String, Class> pair : validEntries) {
              Class clazz = pair.getRight();
              String name = pair.getLeft();
              MajoranaAnnotationRepository maj = getRepo(clazz);
          }
      }

  }

  public static synchronized DbBean getSingletonLazy(Map<String, String> inMap){
      if (singletonLazy==null){
          singletonLazy = new DbBean(inMap);
      }
      return singletonLazy;
  }

    public static synchronized DbBean getSingletonLazy(){
        if (singletonLazy==null){
            singletonLazy = new DbBean();
        }
        return singletonLazy;
    }


    public static synchronized DbBean getSingletonLazy(String url, String driver, String user, String pass){
        if (singletonLazy==null){
            singletonLazy = new DbBean(url, driver, user, pass);
        }
        return singletonLazy;
    }


    @Deprecated
  private DbBean(String url, String driver, String user, String pass){
      cassandraState = new CassandraState(cassandraDbIsPresent.isEnabbled());
      envSetup = new DBEnvSetup(cassandraState, new HashMap<>());
      factory = new MajoranaDBConnectionFactory(envSetup, new CassandraState(true));

      jdbcTemplate = factory.getMainJdbcTemplate().orElse(null);
      cassandraTemplate = factory. getMainCassandraTemplate().orElse( null );
      cassandraIsMain = new CassandraState(factory.getMainVariant() == DatabaseVariant.CASSANDRA );
      if (jdbcTemplate==null){
          LOGGER.warn("Jdbc DB connection "+envSetup.getMainSqlDBName()+" not found");
      }

/*      jdbcTemplate = factory.getJdbcTemplate(factory.getMainDBName()).orElse(null); */

      dbURL = url;
      dbDriver = driver;
      dbUser = user;
      dbPass = pass;
      /*
      if (jdbcTemplate==null){
          LOGGER.warn("DB connection "+factory.getMainDBName()+" not found");
      }

 */
 }


  public boolean connect() throws ClassNotFoundException,SQLException{
          if (dbCon!=null){ return true; }
          try {
              Class.forName(dbDriver);
              MajDatasourceName smn = envSetup.getMainDBName();
              MajDatasourceName casn = envSetup.getMainCassDBName();
              MajDatasourceName jdsn = envSetup.getMainSqlDBName();

              cassDsn = casn;
              jdbcDsn = jdsn;
              mainDsn = smn;

              DBCreds creds = envSetup.getCreds(smn);

              boolean mCass = creds.getVariant() == DatabaseVariant.CASSANDRA;
              cassandraIsMain = new CassandraState(mCass);
              HikariDataSource ds = envSetup.getHikDatasource(envSetup.getMainSqlDBName());
              CqlSession cs = envSetup.getCqlSession(envSetup.getMainCassDBName());

              cassandraDbIsPresent = new CassandraState(cs != null);

              boolean isCass = creds.getVariant() == DatabaseVariant.CASSANDRA;

              dbCon = !isCass && ds != null ? ds.getConnection() : null;
              if (isCass) {
                  if (cassandraDbIsPresent.isEnabbled()) {
                      cassandraTemplate = factory.getCassandraTemplate(casn).orElse(mockCass);
                  }
              }
              jdbcTemplate = factory.getJdbcTemplate(jdsn).orElse(null);
              namedTemplate = factory.getNamedParameterJdbcTemplate(jdsn).orElse(null);
              return ds != null || cs != null;
          } catch (ClassNotFoundException | SQLException e){
              LOGGER.error("Error connecting to db "+e);
              return false;
          }
        }





  public void close() throws SQLException{
      if (dbCon!=null) {
          try {
              dbCon.close();
          } catch (SQLException e){
              LOGGER.warn("Exception closing jdbc connection ",e);
              // throw e;
          }
          dbCon = null;
      }
      if (cassandraTemplate != null){
          CqlSession cs = envSetup.getCqlSession( envSetup.getMainCassDBName());
          try {
              cs.closeAsync();
          } catch (Exception e){
              LOGGER.warn("Exception closing cass cqlSession ",e);
          }
      }

  }


  @Deprecated
  public ResultSet execSQL(String sql) throws SQLException{

                    Statement s = dbCon.createStatement();
                    ResultSet r = s.executeQuery(sql);
                    return (r == null) ? null : r;
                    }

  public void execSQLNoResult(String sql, Object... params){

  }


  public ResultSet execSQL(String sql, Object... params) throws SQLException{
        if (isCass){
            try {
               PreparedStatement pres = cs.prepare(sql);
                BoundStatement bounds = pres.bind(params);
                return  new CassMockResultSet(cs.execute(bounds)).buildMock();
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }

        } else {

            ResultSet rs = null;

            PreparedStatementCreator preparedStatementCreator = new PreparedStatementCreator() {
                String query = sql;

                public java.sql.PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                    java.sql.PreparedStatement ps = connection.prepareStatement(query);
                    for (int i = 0; i < params.length; i++) {
                        ps.setObject(i, params[i]);
                    }
                    return ps;
                }

            };

            final AtomicReference<ResultSet> rs1 = new AtomicReference<>();

            try {

                jdbcTemplate.query(preparedStatementCreator,
                        new ResultSetExtractor<Void>() {
                            @Override
                            public Void extractData(ResultSet rs) throws SQLException, DataAccessException {

                                rs1.set(rs);

                                return null;
                            }
                        });

            } catch (Exception e) {
                LOGGER.warn("Error Executing cql in jdbctemplate " + sql, e);
                throw e;
            }

            return rs1.get();

        }

  }

  public boolean existsSQL(String sql) throws SQLException {
   //   Statement s = dbCon.createStatement();
   //   ResultSet r = s.executeQuery(sql);
      ResultSet r = execSQL(sql, new Object[]{});
      return r.next();
  }

  public int updateSQL(String sql, Object... params) throws SQLException{
      if (isCass){
          try {

             PreparedStatement pres = cs.prepare(sql);
              BoundStatement bounds = pres.bind(params);
              return  cs.execute(bounds).wasApplied() ? 1 : 0;
          } catch (Exception e){
              LOGGER.warn("Error Executing cql in cassandra "+sql,e);
              throw e;
          }

      } else {

          ResultSet rs = null;

          PreparedStatementSetter preparedStatementSetter = new PreparedStatementSetter() {
              String query = sql;

              public void setValues(java.sql.PreparedStatement ps) throws SQLException {
                  for (int i = 0; i < params.length; i++) {
                      ps.setObject(i, params[i]);
                  }
              }

          };

          try {

              return jdbcTemplate.update(sql, preparedStatementSetter);

          } catch (Exception e) {
              LOGGER.warn("Error Executing cql in jdbctemplate " + sql, e);
              throw e;
          }

      }
  }

  public MajoranaAnnotationRepository getRepo(Class beanClass){
      MajoranaAnnotationRepository mj = classDBmap.get(beanClass);

      if (mj==null){
          mj = new MajoranaAnnotationRepository(factory, envSetup.getMainDBName(), beanClass);
          classDBmap.put(beanClass, mj);
      }
      return mj;
  }

    public MajoranaAnnotationRepository getRepo(String entityName){
        Class beanClass = entityFinder.getClassForName(entityName);
        MajoranaAnnotationRepository mj = classDBmap.get(beanClass);

        if (mj==null){
            mj = new MajoranaAnnotationRepository(factory, envSetup.getMainDBName(), beanClass);
            classDBmap.put(beanClass, mj);
        }
        return mj;
    }


    public Object getBean( Class beanClass,  String table,  String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        String sql = mj. getReadString( table,  paramNames, params);
//        String sql = mj.getR

        if (isCass){
            try {
               PreparedStatement pres = cs.prepare(sql);
                BoundStatement bounds = pres.bind(params);
                return cassandraTemplate.selectOne(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                return jdbcTemplate.queryForObject(sql, mj.getMapper());
            } catch (Exception e){
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }

    public Object getBeanNP( Class beanClass,  String table,  String[] paramNames, Object[] params) throws SQLException {
        MajoranaAnnotationRepository mj = getRepo(beanClass);
        String sql = mj. getReadString( table,  paramNames, params);
        if (isCass){
            try {
                PreparedStatement pres = cs.prepare(sql);
                BoundStatement bounds = pres.bind(params);
                return cassandraTemplate.selectOne(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            Map<String, Object> paraMap = getParamMap(paramNames, params);
            try {
                NamedParameterJdbcTemplate npj = factory.getMainNamedParamJdbcTemplate().orElse(null);
                if (npj!=null){ return null; }
                return npj.queryForObject(sql, paraMap, mj.getMapper());
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


    public List<Object> getBeans( Class beanClass,  String table,  String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        
        
        String sql = mj. getReadString( table,  paramNames, params);
//        String sql = mj.getR

        if (isCass){
            try {
               PreparedStatement pres = cs.prepare(sql);
               BoundStatement bounds = pres.bind(params);
                return cassandraTemplate.select(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                return jdbcTemplate.query(sql, mj.getMapper());
            } catch (Exception e){
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }

    public int deleteBeans( Class beanClass,  String table,  String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);



        String sql = mj. getDeleteString( table,  paramNames, params);
//        String sql = mj.getR

        if (isCass){
            try {
               PreparedStatement pres = cs.prepare(sql);
                BoundStatement bounds = pres.bind(params);
               com.datastax.driver.core.ResultSet rs = cs.execute(bounds);
                return rs.all().size();
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                return jdbcTemplate.query(sql, mj.getMapper()).size();
            } catch (Exception e){
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }


    public MultiId storeBean( BaseMajoranaEntity bde) throws SQLException {

      if  (bde==null){
          LOGGER.error("Null Data sent to bean store");
          return new MultiId(0);
      }

        Class beanClass = bde.getClass();

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        String sql = "INSERT INTO "+ bde.getTableName()+" "+mj. getCreateStringNP( bde);
//        String sql = mj.getR

        KeyHolder kh = new org.springframework.jdbc.support.GeneratedKeyHolder();

        if (isCass){
            try {
               PreparedStatement pres = cs.prepare(sql);
                //org.springframework.data.cassandra.core.query.Update bounds =
                        org.springframework.data.cassandra.core.query.Update.empty();
                //Map<String, Object> data = mj. getParameterMapWithDeletedAt(cassDsn, bde);
                //for(Map.Entry<String, Object> en : data.entrySet()){
                //    bounds = bound\s.set( en.getKey(), en.getValue());
                //}
                //        org.springframework.data.cassandra.core.query.Update.addAll( mj.getSqlParameterMapWithDeletedAt(cassDsn,bde))

//                PreparedStatement preparedStatement = cql.prepare("insert into plans (user_id, plan) values (?, ? )");
                BoundStatement insertStatement = pres.bind(mj.getParameterMapWithDeletedAt(cassDsn, bde) );
                
                //CqlTemplate ct = cs.
                com.datastax.driver.core.ResultSet rs = cs.execute(insertStatement);
               Row row  = rs.one();
                if (row==null){ return new MultiId(); }
                return new MultiId(row.getUUID(mj.getKeyUuid()));
                
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

    public MultiId updateBean( MultiId mid,  BaseMajoranaEntity bde) throws SQLException {

        Class beanClass = bde.getClass();

        MajoranaAnnotationRepository mj = getRepo(beanClass);

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
        
        String sql = "UPDATE "+bde.getTableName()+ " "+mj. getUpdateStringNP( bde)
                + " WHERE "+where;
//        String sql = mj.getR

        KeyHolder kh = new org.springframework.jdbc.support.GeneratedKeyHolder();

        if (isCass){
            try {
               PreparedStatement pres = cs.prepare(sql);
                //org.springframework.data.cassandra.core.query.Update bounds =
                org.springframework.data.cassandra.core.query.Update.empty();
                //Map<String, Object> data = mj. getParameterMapWithDeletedAt(cassDsn, bde);
                //for(Map.Entry<String, Object> en : data.entrySet()){
                //    bounds = bounds.set( en.getKey(), en.getValue());
                //}
                //        org.springframework.data.cassandra.core.query.Update.addAll( mj.getSqlParameterMapWithDeletedAt(cassDsn,bde))

//                PreparedStatement preparedStatement = cql.prepare("insert into plans (user_id, plan) values (?, ? )");
                BoundStatement upStatement = pres.bind(mj.getParameterMapWithDeletedAt(cassDsn, bde) );
                //CqlTemplate ct = cs.
                com.datastax.driver.core.ResultSet rs = cs.execute
                       (upStatement);
               Row row  = rs.one();
                if (row==null){ return new MultiId(); }
                return new MultiId(row.getUUID(mj.getKeyUuid()));

                //pres.bind( mj. getSqlParameterSourceWithDeletedAt(bde)); //params);
                // return cassandraTemplate.update(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                int ch = jdbcTemplate.update(sql, mj. getSqlParameterSourceWithDeletedAt( jdbcDsn, bde),kh);
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

    public MultiId deleteBeanById( MultiId mid,  BaseMajoranaEntity bde) throws SQLException {

        Class beanClass = bde.getClass();

        MajoranaAnnotationRepository mj = getRepo(beanClass);

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
               PreparedStatement pres = cs.prepare(sql);
                //org.springframework.data.cassandra.core.query.Update bounds =
                org.springframework.data.cassandra.core.query.Update.empty();
                //Map<String, Object> data = mj. getParameterMapWithDeletedAt(cassDsn, bde);
                //for(Map.Entry<String, Object> en : data.entrySet()){
                //    bounds = bounds.set( en.getKey(), en.getValue());
                //}
                //        org.springframework.data.cassandra.core.query.Update.addAll( mj.getSqlParameterMapWithDeletedAt(cassDsn,bde))

//                PreparedStatement preparedStatement = cql.prepare("insert into plans (user_id, plan) values (?, ? )");
                BoundStatement upStatement = pres.bind(mj.getParameterMapWithDeletedAt(cassDsn, bde) );
                //CqlTemplate ct = cs.
                com.datastax.driver.core.ResultSet rs = cs.execute(upStatement);
               Row row  = rs.one();
                if (row==null){ return new MultiId(); }
                return new MultiId(mid.getUUID());

                //pres.bind( mj. getSqlParameterSourceWithDeletedAt(bde)); //params);
                // return cassandraTemplate.update(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                int ch = jdbcTemplate.update(sql, mj. getSqlParameterSourceWithDeletedAt(jdbcDsn, bde));
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

        MajoranaAnnotationRepository mj = getRepo(beanClass);

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
               PreparedStatement pres = cs.prepare(sql);
                //org.springframework.data.cassandra.core.query.Update bounds =
                org.springframework.data.cassandra.core.query.Update.empty();
                //Map<String, Object> data = mj. getParameterMapWithDeletedAt(cassDsn, bde);
                //for(Map.Entry<String, Object> en : data.entrySet()){
                //    bounds = bounds.set( en.getKey(), en.getValue());
                //}
                //        org.springframework.data.cassandra.core.query.Update.addAll( mj.getSqlParameterMapWithDeletedAt(cassDsn,bde))

//                PreparedStatement preparedStatement = cql.prepare("insert into plans (user_id, plan) values (?, ? )");
                BoundStatement upStatement = pres.bind(mj.getParameterMapWithDeletedAt(cassDsn, bde) );
                //CqlTemplate ct = cs.
                com.datastax.driver.core.ResultSet rs = cs.execute(upStatement);
               Row row  = rs.one();
                if (row==null){ return new MultiId(); }
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

    MajoranaAnnotationRepository mj = getRepo(beanClass);

    if (isCass){
       try {
          PreparedStatement pres = cs.prepare(sql);
           BoundStatement bounds = pres.bind(params);
            return cassandraTemplate.selectOne(bounds, beanClass );
        } catch (Exception e){
          LOGGER.warn("Error Executing cql in cassandra "+sql,e);
          throw e;
        }
  } else {
        try {
            return jdbcTemplate.queryForObject(sql, mj.getMapper());
        } catch (Exception e){
            LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
            throw e;
        }
    }

  }

    public List getBeansArray( Class beanClass, String sql, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        if (isCass){
            try {
               PreparedStatement pres = cs.prepare(sql);
                BoundStatement bounds = pres.bind(params);
                return cassandraTemplate.select(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                return jdbcTemplate.query(sql, mj.getMapper());
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
    
    public List getBeans(Class beanClass, String sql, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);
        if (isCass){
            try {
               PreparedStatement pres = cs.prepare(sql);
                BoundStatement bounds = pres.bind(params);
                return  cassandraTemplate.select(bounds, FloatClass.class );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {       

                return jdbcTemplate.query(sql, mj.getMapper());
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


  public float readFloat( Class beanClass, String sql, List<Object> params) throws Exception {
      if (isCass){
          PreparedStatement pres = cs.prepare(sql);
          BoundStatement bounds = pres.bind(params);
          List<FloatClass> fc = cassandraTemplate.select(bounds, FloatClass.class );
          return fc.get(0).getF();
      } else {
          java.sql.PreparedStatement pstmt = dbCon.prepareStatement(sql);
          
          mapParams(pstmt, params.toArray(new Object[0]));
          ResultSet rs = pstmt.executeQuery();
          rs.next();
          return rs.getFloat(1);
      }
  }

  public long writeJavaObject( Object object, String sql) throws Exception {



        if (isCass){
            String className = object.getClass().getName();
            ByteArrayOutputStream bis = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bis);
            oos.writeObject(object);
            oos.close();
            bis.close();
            ByteBuffer buffer = ByteBuffer.wrap(bis.toByteArray());
            PreparedStatement prepared = getCassPreparedStatement(sql);
            com.datastax.driver.core.ResultSet res = cs.execute(prepared.bind(buffer));
            AtomicLong l = new AtomicLong(0L);
            res.forEach( r->{ l.incrementAndGet();});
            return l.get();
        } else {

            ByteArrayOutputStream bis = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bis);
            oos.writeObject(object);
            oos.close();
            bis.close();
            Blob blob = new SerialBlob(bis.toByteArray());
            java.sql.PreparedStatement pstmt = dbCon.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

            // set input parameters
            pstmt.setBlob(1, blob);
            pstmt.executeUpdate();

            // get the generated key for the id
            ResultSet rs = pstmt.getGeneratedKeys();
            int id = -1;
            if (rs.next()) {
                id = rs.getInt(1);
            }

            rs.close();
            pstmt.close();
//	    System.err.println("writeJavaObject: done serializing: " + className);
            return id;
        }
	  }

	  public Object readJavaObject( String sql, Object[] params) throws Exception {
        if (isCass){
            PreparedStatement pres = cs.prepare(sql);
            BoundStatement bounds = pres.bind(params);
            List<ByteBuffer> fc = cassandraTemplate.select(bounds, ByteBuffer.class);
            ByteBuffer buf = fc.get(0);
            ByteArrayInputStream bais = new ByteArrayInputStream(buf.array());
            ObjectInputStream ois = new ObjectInputStream(bais);
            Object object = ois.readObject();
            String className = object.getClass().getName();
            ois.close();
            bais.close();
            return object;
        } else {
            java.sql.PreparedStatement pstmt = dbCon.prepareStatement(sql);
            mapParams(pstmt, params);
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            Blob blob = rs.getBlob(1);
            ObjectInputStream ois = new ObjectInputStream(blob.getBinaryStream());
            Object object = ois.readObject();
            String className = object.getClass().getName();
            ois.close();
            rs.close();
            pstmt.close();
//	    System.err.println("readJavaObject: done de-serializing: " + className);
            return object;
        }
	  }

}
