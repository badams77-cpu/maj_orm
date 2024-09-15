
package com.majorana.maj_orm.ORM_ACCESS;

import com.majorana.maj_orm.DBs.*;
import com.majorana.maj_orm.ORM.MajoranaAnnotationRepository;
import com.majorana.maj_orm.ORM.MajoranaDBConnectionFactory;
import com.majorana.maj_orm.ORM.MajoranaRepositoryField;
import com.majorana.maj_orm.ORM.BaseMajoranaEntity;
import com.majorana.maj_orm.Utils.MethodPrefixingLoggerFactory;
import com.majorana.maj_orm.DBs.*;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;
import javax.sql.rowset.serial.SerialBlob;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;



public class DbBeanInstance implements DbBeanInstInterface {

    String dbURL = "jdbc:mysql://localhost:3306/distiller";
    String dbDriver = "com.mysql.jdbc.Driver";
    String dbUser = "";
    String dbPass = "";

    private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(DbBeanInstance.class);
    private Connection dbCon;

    private HikariDataSource ds;
    // Cassandra Session
//    private CqlSession cs;

    private MajoranaDBConnectionFactory factory;

    private JdbcTemplate jdbcTemplate;

    private NamedParameterJdbcTemplate namedTemplate;

//    private CassandraTemplate cassandraTemplate;

//    private CqlTemplate cqlTemplate;

    private DBEnvSetup envSetup;

    private CassandraState cassandraState;

    private CassandraState cassandraIsMain;

    private CassandraState cassandraDbIsPresent;

    private Map<Class, MajoranaAnnotationRepository> classDBmap = new HashMap<>();

//    private CassandraTemplate mockCass = null;

    private boolean isCass;

    private EntityFinder entityFinder;

    private static DbBeanInstance singletonLazy;

    private MajDataSourceName mainDsn = null;
    private MajDataSourceName cassDsn = null;
    private MajDataSourceName jdbcDsn = null;
    private int minMinor = 0;
    private int minMajor = 0;

    public String getDbURL() {
        return dbURL;
    }

    // Bean entru
    protected DbBeanInstance() {
        singletonLazy = DbBeanInstance.getSingletonLazy(new CassandraState(false), new HashMap<>());
    }

    public NamedParameterJdbcTemplate getNamedTemplate() {
        return namedTemplate;
    }

//    public CassandraTemplate getCassandraTemplate() {
//        return cassandraTemplate;
//    }

//    public CqlTemplate getCqlTemplate() {
//        return cqlTemplate;
//    }

    public EntityFinder getEntityFinder() {
        return entityFinder;
    }

    public static DbBeanInstance getSingletonLazy() {
        return singletonLazy;
    }

    public MajDataSourceName getMainDsn() {
        return mainDsn;
    }

    public MajDataSourceName getCassDsn() {
        return cassDsn;
    }

    public MajDataSourceName getJdbcDsn() {
        return jdbcDsn;
    }

    protected DbBeanInstance(DBEnvSetup dbEnv) {
        cassandraState = new CassandraState(false);
        entityFinder = new EntityFinder();
        this.envSetup = dbEnv;
        factory = new MajoranaDBConnectionFactory(envSetup, new CassandraState(true));
        if (!factory.getMainCassandraTemplate().isPresent()) {
  //          cassandraTemplate = mock(CassandraTemplate.class);
  //          CqlTemplate cqlTemplate = mock(CqlTemplate.class);
            cassandraIsMain = new CassandraState(false);
        } else {
  //          cassandraTemplate = factory.getMainCassandraTemplate().orElse(null);
  //          CqlTemplate cqlTemplate = (CqlTemplate) cassandraTemplate.getCqlOperations();
 //           cassandraIsMain = new CassandraState(factory.getMainVariant() == DatabaseVariant.CASSANDRA);
        }
        jdbcTemplate = factory.getMainJdbcTemplate().orElse(null);
        namedTemplate = factory.getMainNamedParamJdbcTemplate().orElse(null);

        if (jdbcTemplate == null) {
            LOGGER.warn("Jdbc DB connection " + envSetup.getMainDBName() + " not found");
        }
    }

    protected DbBeanInstance(CassandraState cass, Map<String, String> addMap) {
        cassandraState = cass;
        entityFinder = new EntityFinder();
        envSetup = new DBEnvSetup(cassandraState, addMap);
        factory = new MajoranaDBConnectionFactory(envSetup, new CassandraState(true));
        if (factory.getMainCassandraTemplate() == null) {
   //         cassandraTemplate = mock(CassandraTemplate.class);
  //          cqlTemplate = mock(CqlTemplate.class);
  //          cassandraIsMain = new CassandraState(false);
        } else {
   //         cassandraTemplate = factory.getMainCassandraTemplate().orElse(null);
   //         cqlTemplate = (CqlTemplate) cassandraTemplate.getCqlOperations();
            cassandraIsMain = new CassandraState(factory.getMainVariant() == DatabaseVariant.CASSANDRA);
        }
        jdbcTemplate = factory.getMainJdbcTemplate().orElse(null);
        namedTemplate = factory.getMainNamedParamJdbcTemplate().orElse(null);

        if (jdbcTemplate == null) {
            LOGGER.warn("Jdbc DB coD/Nnnection " + envSetup.getMainDBName() + " not found");
        }
    }

    public DBEnvSetup getEnvSetup() {
        return envSetup;
    }

    /*
  protected DbBeanInstance( CassandraState cass, Map<String, String> addMap){
      cassandraState = cass;
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
*/


//    protected DbBeanInstance(HashMap<String, Object> extra){
//        cassandraState = new CassandraState(false);
//        entityFinder = new EntityFinder();
//        envSetup = new DBEnvSetup(cassandraState, new HashMap<>());
//        factory = new MajoranaDBConnectionFactory(envSetup, new CassandraState(true));
//        jdbcTemplate = factory. getMainJdbcTemplate().orElse(null);
//        cassandraTemplate = factory.getMainCassandraTemplate().orElse(null);
//        cassandraIsMain = new CassandraState(factory.getMainVariant() == DatabaseVariant.CASSANDRA);
//        if (jdbcTemplate==null){
//            LOGGER.warn("Jdbc DB coDNnnection "+envSetup. getMainDBName()+" not found");
//        }
//    }

//    public PreparedStatement getCassPreparedStatement(String sql) {
//        PreparedStatement preparedStatement = cqlTemplate.getSession().prepare(sql);
//        return preparedStatement;
//    }

    public void preloadEntities() {
        entityFinder.setAllowedVersions(envSetup.getDbVersionMajor(), envSetup.getDbVersionMinor());
        entityFinder.populateEntity(envSetup.isCheckDbVersion());
        List<String> pack = entityFinder.getPackages();
        for (String pak : pack) {
            List<Pair<String, Class>> validEntries = entityFinder.getValidEntities();
            for (Pair<String, Class> pair : validEntries) {
                Class clazz = pair.getRight();
                String name = pair.getLeft();
                MajoranaAnnotationRepository maj = getRepo(clazz);
            }
        }

    }



    protected static synchronized DbBeanInstance getSingletonLazy(CassandraState state, Map<String, String> inMap) {
        if (singletonLazy == null) {
            singletonLazy = new DbBeanInstance(state, inMap);
        }
        return singletonLazy;
    }

    protected static synchronized DbBeanInstance getSingletonLazy(boolean state) {
        if (singletonLazy == null) {
            singletonLazy = new DbBeanInstance(new DBEnvSetup(new CassandraState(state), new HashMap<>()));
        }
        return singletonLazy;
    }

    protected static synchronized DbBeanInstance getSingletonLazy(String url, String driver, String user, String pass) {
        if (singletonLazy == null) {
            singletonLazy = new DbBeanInstance(url, driver, user, pass);
        }
        return singletonLazy;
    }


    @Deprecated
    protected DbBeanInstance(String url, String driver, String user, String pass) {
        cassandraState = new CassandraState(cassandraDbIsPresent.isEnabbled());
        envSetup = new DBEnvSetup(cassandraState, new HashMap<>());
        factory = new MajoranaDBConnectionFactory(envSetup, new CassandraState(true));

        jdbcTemplate = factory.getMainJdbcTemplate().orElse(null);
      //  cassandraTemplate = factory.getMainCassandraTemplate().orElse(null);
        cassandraIsMain = new CassandraState(factory.getMainVariant() == DatabaseVariant.CASSANDRA);
        if (jdbcTemplate == null) {
            LOGGER.warn("Jdbc DB connection " + envSetup.getMainSqlDBName() + " not found");
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


    public boolean connect() throws ClassNotFoundException, SQLException {
        if (dbCon != null) {
            return true;
        }
        try {
            if (envSetup == null) {
                envSetup = new DBEnvSetup(new CassandraState(false), new HashMap<>());
            }
            //  Class.forName(dbDriver);
            MajDataSourceName smn = envSetup.getMainDBName();
            MajDataSourceName  casn = envSetup.getMainCassDBName();
            MajDataSourceName jdsn = envSetup.getMainSqlDBName();

            if (factory==null){
                factory = new MajoranaDBConnectionFactory(envSetup, new CassandraState(casn != null));
            } else {
//                 return true
            }

            cassDsn = casn;
            jdbcDsn = jdsn;
            mainDsn = smn;

            DBCreds creds = envSetup.getCreds(smn);

            boolean mCass = creds.getVariant() == DatabaseVariant.CASSANDRA;
            cassandraIsMain = new CassandraState(mCass);
            ds = ds==null ? envSetup.getHikDatasource(envSetup.getMainSqlDBName()) : ds;
            //cs = cs==null ? envSetup.getCqlSession(envSetup.getMainCassDBName()) : cs;

            cassandraDbIsPresent = new CassandraState(false);

            boolean isCass = creds.getVariant() == DatabaseVariant.CASSANDRA;

            dbCon = !isCass && ds != null ? ds.getConnection() : null;
            if (isCass) {
                if (cassandraDbIsPresent.isEnabbled()) {
        //            cassandraTemplate = factory.getCassandraTemplate(casn).orElse(mockCass);
                }
            }
            jdbcTemplate = factory.getJdbcTemplate(jdsn).orElse(null);
            namedTemplate = factory.getNamedParameterJdbcTemplate(jdsn).orElse(null);
            return ds != null;
        } catch (SQLException e) {
            LOGGER.error("Error connecting to db " + e);
            return false;
        }
    }


    public void close() throws SQLException {
        if (dbCon != null) {
            try {
                dbCon.close();
            } catch (SQLException e) {
                LOGGER.warn("Exception closing jdbc connection ", e);
                // throw e;
            }
            dbCon = null;
        }
    //    if (cassandraTemplate != null) {
    //        CqlSession cs = envSetup.getCqlSession(envSetup.getMainCassDBName());
    //        try {
    //            cs.closeAsync();
    //        } catch (Exception e) {
    //            LOGGER.warn("Exception closing cass cqlSession ", e);
    //        }
    //    }

    }


    @Deprecated
    public ResultSet execSQL(String sql) throws SQLException {

        Statement s = dbCon.createStatement();
        ResultSet r = s.executeQuery(sql);
        return (r == null) ? null : r;
    }

    public void execSQLNoResult(String sql, Object... params) {

    }


    public ResultSet execSQL(String sql, Object... params) throws SQLException {
        if (isCass) {
            /*            try {
                PreparedStatement pres = cs.prepare(sql);
                BoundStatement bounds = pres.bind(params);
                return new CassMockResultSet(cqlTemplate.queryForResultSet(bounds)).buildMock();
            } catch (Exception e) {.
                LOGGER.warn("Error Executing cql in cassandra " + sql, e);
                throw e;
            } */
            return null;
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

    public int updateSQL(String sql, Object... params) throws SQLException {
        if (isCass) {
            try {

           //     PreparedStatement pres = cs.prepare(sql);
           //     BoundStatement bounds = pres.bind(params);
           //     return cs.execute(bounds).wasApplied() ? 1 : 0;
            } catch (Exception e) {
                LOGGER.warn("Error Executing cql in cassandra " + sql, e);
                throw e;
            }
            return 0;
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

    public MajoranaRepositoryField getIdField(Class beanClass) {
        MajoranaAnnotationRepository mj = classDBmap.get(beanClass);
        MajoranaRepositoryField maj = mj.getIdField();
        return maj;
    }

    public MajoranaAnnotationRepository getRepo(Class beanClass) {
        MajoranaAnnotationRepository mj = classDBmap.get(beanClass);

        if (mj == null) {
            mj = new MajoranaAnnotationRepository(factory, envSetup.getMainDBName(), beanClass);
            classDBmap.put(beanClass, mj);
        }
        return mj;
    }

    public MajoranaAnnotationRepository getRepo(String entityName) {
        Class beanClass = entityFinder.getClassForName(entityName);
        MajoranaAnnotationRepository mj = classDBmap.get(beanClass);

        if (mj == null) {
            mj = new MajoranaAnnotationRepository(factory, envSetup.getMainDBName(), beanClass);
            classDBmap.put(beanClass, mj);
        }
        return mj;
    }


    @Override
    public Object getBeanNP(Class beanClass, String table, String sql, String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);
//        String sql = mj.getR

        if (isCass) {
            try {
    //            PreparedStatement pres = cs.prepare(sql);
   //             BoundStatement bounds = pres.bind(params);
    //            return cassandraTemplate.selectOne(bounds, beanClass);
            } catch (Exception e) {
                LOGGER.warn("Error Executing cql in cassandra " + sql, e);
                throw e;
            }
            return null;
        } else {
            Map<String, Object> mp = getParamMap(paramNames, params);
            SqlParameterSource sps = new MapSqlParameterSource(mp);

            try {
                return namedTemplate.queryForObject(sql, sps, mj.getMapper());
            } catch (Exception e) {
                LOGGER.warn("Error Executing sql in jdbc template " + sql, e);
                throw e;
            }
        }

    }

    @Override
    public Object getBeanNPWithSelectClause(Class beanClass, String table, String sql1, String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        String sql = mj.getReadStringNPSelectClause(table, sql1, paramNames, params);
//        String sql = mj.getR

        if (isCass) {
            try {
        //        PreparedStatement pres = cs.prepare(sql);
        //        BoundStatement bounds = pres.bind(params);
        //        return cassandraTemplate.selectOne(bounds, beanClass);
                return null;
            } catch (Exception e) {
                LOGGER.warn("Error Executing cql in cassandra " + sql, e);
                throw e;
            }
        } else {
            Map<String, Object> mp = getParamMap(paramNames, params);
            SqlParameterSource sps = new MapSqlParameterSource(mp);

            try {
                return namedTemplate.queryForObject(sql, sps, mj.getMapper());
            } catch (Exception e) {
                LOGGER.warn("Error Executing sql in jdbc template " + sql, e);
                throw e;
            }
        }

    }


    public Object getBean(Class beanClass, String table, String sql1, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        String sql = mj.getReadStringSelectClause(table, sql1);
//        String sql = mj.getR

        if (isCass) {
            try {
 //               PreparedStatement pres = cs.prepare(sql);
 //               BoundStatement bounds = pres.bind(params);
 //               return cassandraTemplate.selectOne(bounds, beanClass);
            } catch (Exception e) {
                LOGGER.warn("Error Executing cql in cassandra " + sql, e);
                throw e;
            }
            return null;
        } else {
            try {
                return jdbcTemplate.queryForObject(sql, params, mj.getMapper());
            } catch (Exception e) {
                LOGGER.warn("Error Executing sql in jdbc template " + sql, e);
                throw e;
            }
        }

    }

    public List<Object> getBeans(Class beanClass, String table, String sql1, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        String sql = mj.getReadStringSelectClause(table, sql1);
//        String sql = mj.getR

        if (isCass) {
            try {
    //            PreparedStatement pres = cs.prepare(sql);
    //            BoundStatement bounds = pres.bind(params);
    //            return cassandraTemplate.select(bounds, beanClass);
            } catch (Exception e) {
                LOGGER.warn("Error Executing cql in cassandra " + sql, e);
                throw e;
            }
            return null;
        } else {
            try {
                return jdbcTemplate.query(sql, params, mj.getMapper());
            } catch (Exception e) {
                LOGGER.warn("Error Executing sql in jdbc template " + sql, e);
                throw e;
            }
        }

    }

    public Object[] getBeansArrayNP(Class beanClass, String table, String sql, String[] paramNames, Object[] params) throws SQLException {
        List<Object> obj = getBeansNP(beanClass, table, sql, paramNames, params);
        return obj.stream().toArray();
    }

    public Object[] getBeansArray(Class beanClass, String table, String sql, Object[] params) throws SQLException {
        List<Object> obj = getBeans(beanClass, table, sql, params);
        return obj.stream().toArray();
    }


    public Map<String, Object> getParamMap(String named[], Object params[]) {
        HashMap<String, Object> ret = new HashMap<>();
        int len = Math.min(named.length, params.length);
        for (int i = 0; i < len; i++) {
            ret.put(named[i], params[i]);
        }
        return ret;
    }

    public List<Object> getBeansNPWithSelectClause(Class beanClass, String table, String sql1, String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);


        String sql = mj.getReadStringNPSelectClause(table, sql1, paramNames, params);
//        String sql = mj.getR

        if (isCass) {
            try {

        //        PreparedStatement pres = cs.prepare(sql);
        //        BoundStatement bounds = pres.bind(params);
        //        return cassandraTemplate.select(bounds, beanClass);
            } catch (Exception e) {
                LOGGER.warn("Error Executing cql in cassandra " + sql, e);
                throw e;
            }
            return null;
        } else {
            try {
                MapSqlParameterSource src = new MapSqlParameterSource(getParamMap(paramNames, params));
                return namedTemplate.query(sql, src, mj.getMapper());
            } catch (Exception e) {
                LOGGER.warn("Error Executing sql in jdbc template " + sql, e);
                throw e;
            }
        }

    }

    public List<Object> getBeansNP(Class beanClass, String table, String sql, String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);
//        String sql = mj.getR

        if (isCass) {
            try {
        //        PreparedStatement pres = cs.prepare(sql);
        //        BoundStatement bounds = pres.bind(params);
        //        return cassandraTemplate.select(bounds, beanClass);
            } catch (Exception e) {
                LOGGER.warn("Error Executing cql in cassandra " + sql, e);
                throw e;
            }
            return null;
        } else {
            try {
                MapSqlParameterSource src = new MapSqlParameterSource(getParamMap(paramNames, params));
                return namedTemplate.query(sql, src, mj.getMapper());
            } catch (Exception e) {
                LOGGER.warn("Error Executing sql in jdbc template " + sql, e);
                throw e;
            }
        }

    }


    public int deleteBeans(Class beanClass, String table, String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);


        String sql = mj.getDeleteString(table, paramNames, params);
//        String sql = mj.getR

        if (isCass) {
            try {
        //        PreparedStatement pres = cs.prepare(sql);
        //        BoundStatement bounds = pres.bind(params);
        //        com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute(bounds);
        //        return rs.all().size();
            } catch (Exception e) {
                LOGGER.warn("Error Executing cql in cassandra " + sql, e);
                throw e;
            }
            return 0;
        } else {
            try {
                return jdbcTemplate.query(sql, mj.getMapper()).size();
            } catch (Exception e) {
                LOGGER.warn("Error Executing sql in jdbc template " + sql, e);
                throw e;
            }
        }

    }


    public int deleteBeanById(Class<? extends BaseMajoranaEntity> beanClass, String table, MultiId id) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        MajoranaRepositoryField idField = mj.getIdField();

        String[] paramNames = new String[1];
        Object[] params = new Object[1];

        paramNames[0] = idField.getDbColumn();
        params[0] = idField.getValueType()!= UUID.class ? id.getId() : id.getUUID();

        String sql = mj.getDeleteString(table, paramNames, params);
//        String sql = mj.getR

        if (isCass) {
            try {
         //       PreparedStatement pres = cs.prepare(sql);
         //       BoundStatement bounds = pres.bind(params);
          //      com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute(bounds);
         //       return rs.all().size();
            } catch (Exception e) {
                LOGGER.warn("Error Executing cql in cassandra " + sql, e);
                throw e;
            }
            return 0;
        } else {
            try {
                return jdbcTemplate.query(sql, mj.getMapper()).size();
            } catch (Exception e) {
                LOGGER.warn("Error Executing sql in jdbc template " + sql, e);
                throw e;
            }
        }

    }


    public MultiId storeBean(BaseMajoranaEntity bde) throws SQLException {

        if (bde == null) {
            LOGGER.error("Null Data sent to bean store");
            return new MultiId(0);
        }

        Class beanClass = bde.getClass();

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        String sql = "INSERT INTO " + bde.getTableName() + " " + mj.getCreateStringNP(bde);
//        String sql = mj.getR

        KeyHolder kh = new org.springframework.jdbc.support.GeneratedKeyHolder();

        if (isCass) {
      //      try {
     //           PreparedStatement pres = cs.prepare(sql);
                //org.springframework.data.cassandra.core.query.Update bounds =
     //           org.springframework.data.cassandra.core.query.Update.empty();
                //Map<String, Object> data = mj. getParameterMapWithDeletedAt(cassDsn, bde);
                //for(Map.Entry<String, Object> en : data.entrySet()){
                //    bounds = bound\s.set( en.getKey(), en.getValue());
                //}
                //        org.springframework.data.cassandra.core.query.Update.addAll( mj.getSqlParameterMapWithDeletedAt(cassDsn,bde))

//                PreparedStatement preparedStatement = cql.prepare("insert into plans (user_id, plan) values (?, ? )");
  //              BoundStatement insertStatement = pres.bind(mj.getParameterMapWithDeletedAt(cassDsn, bde));

                //CqlTemplate ct = cs.
    //            com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute(insertStatement);
    //            Row row = rs.one();
     //           if (row == null) {
     //               return new MultiId();
    //            }
                return new MultiId(getUUID(mj.getKeyUuid()));

                //pres.bind( mj. getSqlParameterSourceWithDeletedAt(bde)); //params);
                // return cassandraTemplate.update(bounds, beanClass );
    //        } catch (Exception e) {
    //            LOGGER.warn("Error Executing cql in cassandra " + sql, e);
    //            throw e;
    //        }
        } else {
            try {
                int ch = factory.getNamedParameterJdbcTemplate(jdbcDsn).orElseThrow(
                        RuntimeException::new
                ).update(sql, mj.getSqlParameterSourceWithDeletedAt(jdbcDsn, bde), kh);
                if (ch != 0) {
                    Number n = kh.getKey();
                    if (n != null) {
                        return new MultiId(n.intValue());
                    }
                }
                return new MultiId();
            } catch (Exception e) {
                LOGGER.warn("Error Update sql in jdbc template " + sql, e);
                throw e;
            }
        }

    }

    public MultiId updateBean(MultiId mid, BaseMajoranaEntity bde) throws SQLException {

        Class beanClass = bde.getClass();

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        List<MajoranaRepositoryField> mrf = mj.getRepoFields();

        String idCol = mj.getKeyId();
        String uuidCol = mj.getKeyUuid();

        if (!mid.hasAnyId()) {
            LOGGER.warn("Not updating " + bde.getTableName() + "  no id given");
            return mid;
        }

        //      String where = mid.hasId() ? idCol+"="+mid.getId() : "";
        //      where = where + (mid.hasBoth() ? " AND " : "");
        //      where = where + (mid.hasUuid() ? uuidCol = "'"+mid.getUUID().toString()+"'"
        //              : "");
        if (mrf.stream().filter(x->x.isUpdateable()).count() == 0L) {
            LOGGER.warn(bde.getTableName() + " has no updateable fields to update");
            return new MultiId();
        }

        String sql = "UPDATE " + bde.getTableName() + " " + mj.getUpdateStringNP(bde);
        //               + " WHERE "+where;


        KeyHolder kh = new org.springframework.jdbc.support.GeneratedKeyHolder();

        Map<String, Object> paramMap = mj.getParameterMapWithDeletedAt(isCass ? cassDsn : jdbcDsn, bde);

        List<MajoranaRepositoryField> ids = mrf.stream()
                .filter(f->f.isAltId() || f.isId())
        .collect(Collectors.toUnmodifiableList());

        for( MajoranaRepositoryField idField : ids) {
            Class fieldType = idField.getValueType();
            if (fieldType != UUID.class && !(fieldType.isPrimitive() && !(Number.class.isAssignableFrom(fieldType)))) {
                LOGGER.warn(bde.getTableName() + " has no id field was " + fieldType.getCanonicalName());
                return new MultiId();
            } else {
                paramMap.put(idField.getDbColumn(), idField.getValueType() == UUID.class ? mid.getUUID() : mid.getId());
            }
        }
            if (isCass) {
        //        try {
       //             PreparedStatement pres = cs.prepare(sql);
                    //org.springframework.data.cassandra.core.query.Update bounds =
         //           org.springframework.data.cassandra.core.query.Update.empty();
                    //Map<String, Object> data = mj. getParameterMapWithDeletedAt(cassDsn, bde);
                    //for(Map.Entry<String, Object> en : data.entrySet()){
                    //    bounds = bounds.set( en.getKey(), en.getValue());
                    //}
                    //        org.springframework.data.cassandra.core.query.Update.addAll( mj.getSqlParameterMapWithDeletedAt(cassDsn,bde))

//                PreparedStatement preparedStatement = cql.prepare("insert into plans (user_id, plan) values (?, ? )");
           //         BoundStatement upStatement = pres.bind(paramMap);
                    //CqlTemplate ct = cs.
            //        com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute
            //                (upStatement);
            //        Row row = rs.one();
            //        if (row == null) {
            //            return new MultiId();
          //          }
                    return new MultiId(getUUID(mj.getKeyUuid()));

                    //pres.bind( mj. getSqlParameterSourceWithDeletedAt(bde)); //params);
                    // return cassandraTemplate.update(bounds, beanClass );
          //      } catch (Exception e) {
          //          LOGGER.warn("Error Executing cql in cassandra " + sql, e);
          //          throw e;
          //      }
            } else {
                try {
                    int ch = namedTemplate.update(sql, new MapSqlParameterSource(paramMap), kh);
                    if (ch != 0) {
                        Number n = kh.getKey();
                        if (n != null) {
                            return new MultiId(n.intValue());
                        }
                    }
                    return new MultiId();
                } catch (Exception e) {
                    LOGGER.warn("Error Update sql in jdbc template " + sql, e);
                    throw e;
                }
            }

        }


    public int updateBeanNP(BaseMajoranaEntity bde, String sqlWhereParam[], Object sqlId[],  String[] paramNames, Object[] params) throws SQLException {

        Class beanClass = bde.getClass();

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        List<MajoranaRepositoryField> mrf = mj.getRepoFields();


        String sqlWhere =  Arrays.stream(sqlWhereParam).filter( pn->
                        mj.getRepoFields().stream().anyMatch( f-> ((MajoranaRepositoryField) f).getDbColumn().equals(pn)))
                .map( f-> f+"=:"+f).collect(Collectors.joining(" and "));

        String sql = "UPDATE " + bde.getTableName() +" SET "+
                Arrays.stream(paramNames).filter( pn-> mj.getRepoFields().stream().anyMatch( f-> ((MajoranaRepositoryField) f)
                        .getDbColumn().equals(pn)))
                        .map( f->f+"=:"+f) .collect(Collectors.joining(", ")) + " WHERE "+sqlWhere;
        //               + " WHERE "+where;
//        String sql = mj.getR

        KeyHolder kh = new org.springframework.jdbc.support.GeneratedKeyHolder();

        Map<String, Object> paramMap = getParamMap(paramNames, params);

        paramMap.putAll(getParamMap(sqlWhereParam, sqlId));

        if (isCass) {
            try {
            //    PreparedStatement pres = cs.prepare(sql);
                //org.springframework.data.cassandra.core.query.Update bounds =
            //    org.springframework.data.cassandra.core.query.Update.empty();
                //Map<String, Object> data = mj. getParameterMapWithDeletedAt(cassDsn, bde);
                //for(Map.Entry<String, Object> en : data.entrySet()){
                //    bounds = bounds.set( en.getKey(), en.getValue());
                //}
                //        org.springframework.data.cassandra.core.query.Update.addAll( mj.getSqlParameterMapWithDeletedAt(cassDsn,bde))

//                PreparedStatement preparedStatement = cql.prepare("insert into plans (user_id, plan) values (?, ? )");
            //    BoundStatement upStatement = pres.bind(paramMap);
                //CqlTemplate ct = cs.
            //    com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute
            //            (upStatement);
            //    Row row = rs.one();
           //     if (row == null) {
           //         return 0;
           //     }
                return 1;

                //pres.bind( mj. getSqlParameterSourceWithDeletedAt(bde)); //params);
                // return cassandraTemplate.update(bounds, beanClass );
            } catch (Exception e) {
                LOGGER.warn("Error Executing cql in cassandra " + sql, e);
                throw e;
            }
        } else {
            try {
                int ch = namedTemplate.update(sql, new MapSqlParameterSource(paramMap), kh);
                if (ch != 0) {
                    Number n = kh.getKey();
                    if (n != null) {
                        return n.intValue();
                    }
                }
                return  0;
            } catch (Exception e) {
                LOGGER.warn("Error Update sql in jdbc template " + sql, e);
                throw e;
            }
        }

    }



        private UUID getUUID (String s){
            return UUID.fromString(s);
        }


    public MultiId deleteBeanById( MultiId mid,  BaseMajoranaEntity bde) throws SQLException {

        Class beanClass = bde.getClass();

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        List<MajoranaRepositoryField> mrf = mj.getRepoFields();

        String idCol = mj.getIdField().getDbColumn();
        String uuidCol = mj.getKeyUuid();

        if (!mid.hasAnyId()){
            LOGGER.warn("Not updating "+bde.getTableName()+ "  no id given");
            return mid;
        }

//        String where = mid.hasId() ? idCol+"="+mid.getId() : "";
//        where = where + (mid.hasBoth() ? " AND " : "");
//        where = where + (mid.hasUuid() ? uuidCol = "'"+mid.getUUID().toString()+"'"
//                : "");

        String where = idCol + " = :"+idCol;

        String sql = "DELETE from "+bde.getTableName()
                + " WHERE "+where;

        List<MajoranaRepositoryField> ids = mrf.stream()
                .filter(f->f.isAltId() || f.isId())
                .collect(Collectors.toUnmodifiableList());


        Map<String, Object> paramMap = mj.getParameterMapWithDeletedAt(isCass ? cassDsn : jdbcDsn, bde);

        for( MajoranaRepositoryField idField : ids) {
            Class fieldType = idField.getValueType();
            if (fieldType != UUID.class && !(fieldType.isPrimitive() && !(Number.class.isAssignableFrom(fieldType)))) {
                LOGGER.warn(bde.getTableName() + " has no id field was " + fieldType.getCanonicalName());
                return new MultiId();
            } else {
                paramMap.put(idField.getDbColumn(), idField.getValueType() == UUID.class ? mid.getUUID() : mid.getId());
            }
        }
//        String sql = mj.getR

//        KeyHolder kh = new org.springframework.jdbc.support.GeneratedKeyHolder();

        if (isCass){
            try {
     //          PreparedStatement pres = cs.prepare(sql);
                //org.springframework.data.cassandra.core.query.Update bounds =
      //          org.springframework.data.cassandra.core.query.Update.empty();
                //Map<String, Object> data = mj. getParameterMapWithDeletedAt(cassDsn, bde);
                //for(Map.Entry<String, Object> en : data.entrySet()){
                //    bounds = bounds.set( en.getKey(), en.getValue());
                //}
                //        org.springframework.data.cassandra.core.query.Update.addAll( mj.getSqlParameterMapWithDeletedAt(cassDsn,bde))

//                PreparedStatement preparedStatement = cql.prepare("insert into plans (user_id, plan) values (?, ? )");
        //        BoundStatement upStatement = pres.bind(paramMap );
                //CqlTemplate ct = cs.
          //      com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute(upStatement);
         //      Row row  = rs.one();
         //       if (row==null){ return new MultiId(); }
         //       return new MultiId(mid.getUUID());

                //pres.bind( mj. getSqlParameterSourceWithDeletedAt(bde)); //params);
                // return cassandraTemplate.update(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
            return null;
        } else {
            try {
                int ch = namedTemplate.update(sql, new MapSqlParameterSource(paramMap));
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
          //     PreparedStatement pres = cs.prepare(sql);
                //org.springframework.data.cassandra.core.query.Update bounds =
           //     org.springframework.data.cassandra.core.query.Update.empty();
                //Map<String, Object> data = mj. getParameterMapWithDeletedAt(cassDsn, bde);
                //for(Map.Entry<String, Object> en : data.entrySet()){
                //    bounds = bounds.set( en.getKey(), en.getValue());
                //}
                //        org.springframework.data.cassandra.core.query.Update.addAll( mj.getSqlParameterMapWithDeletedAt(cassDsn,bde))

//                PreparedStatement preparedStatement = cql.prepare("insert into plans (user_id, plan) values (?, ? )");
             //   BoundStatement upStatement = pres.bind(mj.getParameterMapWithDeletedAt(cassDsn, bde) );
                //CqlTemplate ct = cs.
               // com.datastax.oss.driver.api.core.cql.ResultSet rs = cs.execute(upStatement);
              // Row row  = rs.one();
              //  if (row==null){ return new MultiId(); }
              //  return new MultiId(mid.getUUID());

                //pres.bind( mj. getSqlParameterSourceWithDeletedAt(bde)); //params);
                // return cassandraTemplate.update(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
            return null;
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

    public Object[] getBeansArray( Class beanClass, String sql, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        if (isCass){
            try {
    //           PreparedStatement pres = cs.prepare(sql);
    //            BoundStatement bounds = pres.bind(params);
    //            return cassandraTemplate.select(bounds, beanClass ).toArray();
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
            return null;
        } else {
            try {
                return jdbcTemplate.query(sql, mj.getMapper()).toArray();
            } catch (Exception e){
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }


    public List<Object> getBeansNP(Class beanClass, String sql, String[] paramNames, Object[] params) {
        MajoranaAnnotationRepository mj = getRepo(beanClass);

        if (isCass){
            try {
      //          PreparedStatement pres = cs.prepare(sql);
      //          BoundStatement bounds = pres.bind(params);
      //          return cassandraTemplate.select(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
            return null;
        } else {
            SqlParameterSource sps = new MapSqlParameterSource(getParamMap(paramNames, params));
            try {
                return namedTemplate.query(sql, sps, mj.getMapper());
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
        //       PreparedStatement pres = cs.prepare(sql);
        //        BoundStatement bounds = pres.bind(params);
        //        return  cassandraTemplate.select(bounds, beanClass );
            } catch (Exception e){
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
            return null;
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
   //       PreparedStatement pres = cs.prepare(sql);
   //       BoundStatement bounds = pres.bind(params);
   //       List<FloatClass> fc = cassandraTemplate.select(bounds, FloatClass.class );
    //      return fc.get(0).getF()
          return 0.0f;
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
//            String className = object.getClass().getName();
//            ByteArrayOutputStream bis = new ByteArrayOutputStream();
//            ObjectOutputStream oos = new ObjectOutputStream(bis);
//            oos.writeObject(object);
//            oos.close();
//            bis.close();
//            ByteBuffer buffer = ByteBuffer.wrap(bis.toByteArray());
//            PreparedStatement prepared = getCassPreparedStatement(sql);
//            com.datastax.oss.driver.api.core.cql.ResultSet res = cs.execute(prepared.bind(buffer));
//            AtomicLong l = new AtomicLong(0L);
//            res.forEach( r->{ l.incrementAndGet();});
//            return l.get();
            return 0;
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
//            PreparedStatement pres = cs.prepare(sql);
//            BoundStatement bounds = pres.bind(params);
//            List<ByteBuffer> fc = cassandraTemplate.select(bounds, ByteBuffer.class);
//            ByteBuffer buf = fc.get(0);
//            ByteArrayInputStream bais = new ByteArrayInputStream(buf.array());
//            ObjectInputStream ois = new ObjectInputStream(bais);
//            Object object = ois.readObject();
//            String className = object.getClass().getName();
//            ois.close();
//            bais.close();
//            return object;
            return null;
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



      // Proected Getters


    protected Connection getDbCon() {
        return dbCon;
    }

    protected HikariDataSource getDs() {
        return ds;
    }

//    protected CqlSession getCs() {
//        return cs;
//    }

    protected MajoranaDBConnectionFactory getFactory() {
        return factory;
    }

    protected JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    protected CassandraState getCassandraState() {
        return cassandraState;
    }

    protected CassandraState getCassandraIsMain() {
        return cassandraIsMain;
    }

    protected CassandraState getCassandraDbIsPresent() {
        return cassandraDbIsPresent;
    }

    protected Map<Class, MajoranaAnnotationRepository> getClassDBmap() {
        return classDBmap;
    }

//    protected CassandraTemplate getMockCass() {
//        return mockCass;
//    }

    protected boolean isCass() {
        return isCass;
    }
}
