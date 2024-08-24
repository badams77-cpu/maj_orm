
package com.majorana.ORM_ACCESS;

import com.majorana.DBs.*;
import com.majorana.ORM.CassMockResultSet;
import com.majorana.ORM.MajoranaAnnotationRepository;
import com.majorana.ORM.MajoranaDBConnectionFactory;
import com.majorana.Utils.MethodPrefixingLoggerFactory;
import com.com.majorana.ORM.*;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.rowset.serial.SerialBlob;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

/**
 * Quick access to any of database methods
 */

public class DbBean {

  private String dbURL = "jdbc:mysql://localhost:3306/Majorana";
  private String dbDriver = "com.mysql.jdbc.Driver";
  private String dbUser = "";
  private String dbPass = "";

  private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(DbBean.class);
  private Connection dbCon;

  private HikariDataSource ds;

  private CqlSession cs;

  private MajoranaDBConnectionFactory factory;

  private JdbcTemplate jdbcTemplate;

  private CassandraTemplate cassandraTemplate;

  private DBEnvSetup envSetup ;

  private CassandraState cassandraState;

  private CassandraState cassandraIsMain;

  private CassandraState cassandraDbIsPresent;

  private Map<Class, MajoranaAnnotationRepository> classDBmap = new HashMap<>();

  private CassandraTemplate  mockCass = mock(CassandraTemplate.class);

  private boolean isCass;

  private static DbBean singletonLazy;

  private  MajDatasourceName mainDsn = null;
  private  MajDatasourceName cassDsn = null;
  private  MajDatasourceName jdbcDsn = null;

    /**
     * Bare constructor, read vars from environment, orivate
     */


  private DbBean(){
      cassandraState = new CassandraState(false);
      envSetup = new DBEnvSetup(cassandraState);
      factory = new MajoranaDBConnectionFactory(envSetup, new CassandraState(true));
      jdbcTemplate = factory. getMainJdbcTemplate().orElse(null);
      cassandraTemplate = factory.getMainCassandraTemplate().orElse(null);
      cassandraIsMain = new CassandraState(factory.getMainVariant() == DatabaseVariant.CASSANDRA);
      if (jdbcTemplate==null){
          LOGGER.warn("Jdbc DB coDNnnection "+envSetup. getMainDBName()+" not found");
      }
  }

    /**
     * Creates a new DbBean signleton if needed
     * @return
     */

  public static synchronized DbBean getSingletonLazy(){
      if (singletonLazy==null){
          singletonLazy = new DbBean();
      }
      return singletonLazy;
  }

    /**
     *
     * Creates a new DbBean singleton if needed with given credentials
     *
     * @param url
     * @param driver
     * @param user
     * @param pass
     * @return
     */

    public static synchronized DbBean getSingletonLazy(String url, String driver, String user, String pass){
        if (singletonLazy==null){
            singletonLazy = new DbBean(url, driver, user, pass);
        }
        return singletonLazy;
    }

    /**
     * Creates a new DbBean with credentials

    */
    @Deprecated
  private DbBean(String url, String driver, String user, String pass){
      cassandraState = new CassandraState(cassandraDbIsPresent.isEnabbled());
      envSetup = new DBEnvSetup(cassandraState);
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

    /**
     *  COnnected a DB bean if needed
     *
     *
      * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */


  public boolean connect() throws ClassNotFoundException,SQLException{
          if (dbCon!=null){ return true; }
          try {
              Class.forName(dbDriver);
              Majorana.DBs.MajDatasourceName smn = envSetup.getMainDBName();
              Majorana.DBs.MajDatasourceName casn = envSetup.getMainCassDBName();
              Majorana.DBs.MajDatasourceName jdsn = envSetup.getMainSqlDBName();

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
              return ds != null || cs != null;
          } catch (ClassNotFoundException | SQLException e){
              LOGGER.error("Error connecting to db "+e);
              return false;
          }
        }


    /**
     * close out the DNB
     * @throws SQLException
     */


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

  public ResultSet execSQL(String sql, Object... params) throws SQLException{
        if (isCass){
            try {
                com.datastax.oss.driver.api.core.cql.PreparedStatement pres = cs.prepare(sql);
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

                public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                    PreparedStatement ps = connection.prepareStatement(query);
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

              com.datastax.oss.driver.api.core.cql.PreparedStatement pres = cs.prepare(sql);
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

              public void setValues(PreparedStatement ps) throws SQLException {
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


    public Object getBean( Class beanClass,  String table,  String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        String sql = mj. getReadString( table,  paramNames, params);
//        String sql = mj.getR

        if (isCass){
            try {
                com.datastax.oss.driver.api.core.cql.PreparedStatement pres = cs.prepare(sql);
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

    public List<Object> getBeans( Class beanClass,  String table,  String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        
        
        String sql = mj. getReadString( table,  paramNames, params);
//        String sql = mj.getR

        if (isCass){
            try {
                com.datastax.oss.driver.api.core.cql.PreparedStatement pres = cs.prepare(sql);
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
                com.datastax.oss.driver.api.core.cql.PreparedStatement pres = cs.prepare(sql);
                BoundStatement bounds = pres.bind(params);
                com.datastax.oss.driver.api.core.cql.ResultSet rs = cassandraTemplate.execute(bounds);
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


    public MultiId storeBean(    com.majorana.ORM.BaseMajoranaEntity bde) throws SQLException {

        Class beanClass = bde.getClass();

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        String sql = "INSERT INTO "+ bde.getTableName()+" "+mj. getCreateStringNP( bde);
//        String sql = mj.getR

        KeyHolder kh = new org.springframework.jdbc.support.GeneratedKeyHolder();

        if (isCass){
            try {
                com.datastax.oss.driver.api.core.cql.PreparedStatement pres = cs.prepare(sql);
                //org.springframework.data.cassandra.core.query.Update bounds =
                        org.springframework.data.cassandra.core.query.Update.empty();
                //Map<String, Object> data = mj. getParameterMapWithDeletedAt(cassDsn, bde);
                //for(Map.Entry<String, Object> en : data.entrySet()){
                //    bounds = bounds.set( en.getKey(), en.getValue());
                //}
                //        org.springframework.data.cassandra.core.query.Update.addAll( mj.getSqlParameterMapWithDeletedAt(cassDsn,bde))

//                PreparedStatement preparedStatement = cql.prepare("insert into plans (user_id, plan) values (?, ? )");
                BoundStatement insertStatement = pres.bind(mj.getParameterMapWithDeletedAt(cassDsn, bde) );
                //CqlTemplate ct = cs.
                com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute(insertStatement);
                com.datastax.oss.driver.api.core.cql.Row row  = rs.one();
                if (row==null){ return new MultiId(); }
                return new MultiId(row.getUuid(mj.getKeyUuid()));
                
                //pres.bind( mj. getSqlParameterSourceWithDeletedAt(bde)); //params);
                // return cassandraTemplate.update(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                int ch = jdbcTemplate.update(sql, mj. getSqlParameterSourceWithDeletedAt(jdbcDsn, bde),kh);
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

    public MultiId updateBean( MultiId mid,  com.majorana.ORM.BaseMajoranaEntity bde) throws SQLException {

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
                com.datastax.oss.driver.api.core.cql.PreparedStatement pres = cs.prepare(sql);
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
                com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute(upStatement);
                com.datastax.oss.driver.api.core.cql.Row row  = rs.one();
                if (row==null){ return new MultiId(); }
                return new MultiId(row.getUuid(mj.getKeyUuid()));

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

    public MultiId deleteBeanById( MultiId mid,  com.majorana.ORM.BaseMajoranaEntity bde) throws SQLException {

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
                com.datastax.oss.driver.api.core.cql.PreparedStatement pres = cs.prepare(sql);
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
                com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute(upStatement);
                com.datastax.oss.driver.api.core.cql.Row row  = rs.one();
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

    public MultiId deleteBeanByParams( MultiId mid,  com.majorana.ORM.BaseMajoranaEntity bde) throws SQLException {

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
                com.datastax.oss.driver.api.core.cql.PreparedStatement pres = cs.prepare(sql);
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
                com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute(upStatement);
                com.datastax.oss.driver.api.core.cql.Row row  = rs.one();
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
           com.datastax.oss.driver.api.core.cql.PreparedStatement pres = cs.prepare(sql);
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
                com.datastax.oss.driver.api.core.cql.PreparedStatement pres = cs.prepare(sql);
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


    public List getBeans( Class beanClass, String sql, Object... params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        if (isCass){
            try {
                com.datastax.oss.driver.api.core.cql.PreparedStatement pres = cs.prepare(sql);
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


  public float readFloat( String sql) throws Exception {
	    PreparedStatement pstmt = dbCon.prepareStatement(sql);
	    ResultSet rs = pstmt.executeQuery();
	    rs.next();
	    return rs.getFloat(1);
  }

  public long writeJavaObject( Object object, String sql) throws Exception {
	    String className = object.getClass().getName();
	    ByteArrayOutputStream bis = new ByteArrayOutputStream();
	    ObjectOutputStream oos = new ObjectOutputStream(bis);
	    oos.writeObject(object);
	    oos.close();
	    bis.close();
	    Blob blob = new SerialBlob(bis.toByteArray());
	    PreparedStatement pstmt = dbCon.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);

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

	  public Object readJavaObject( String sql) throws Exception {
	    PreparedStatement pstmt = dbCon.prepareStatement(sql);
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
