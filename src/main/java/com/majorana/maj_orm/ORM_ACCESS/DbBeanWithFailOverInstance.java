package com.majorana.ORM_ACCESS;

import com.majorana.DBs.*;
import com.majorana.ORM.MajoranaAnnotationRepository;
import com.majorana.ORM.MajoranaDBConnectionFactory;
import com.majorana.Utils.MethodPrefixingLoggerFactory;
import com.majorana.ORM.BaseMajoranaEntity;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.mock;

public class DbBeanWithFailOverInstance implements DbBeanWithFailOverInterface {

    private DbBean last;

    private DbBeanWithFailOverInstance next;

    private MajDataSourceName name;

    private MajDataSource source;

    private  MajoranaDBConnectionFactory factory;

    private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(DbBeanInstance.class);
    private Connection dbCon;

    private HikariDataSource ds;
    // Cassandra Session
    private CqlSession cs;
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

    private static DbBeanInstance singletonLazy;

    private  MajDataSourceName mainDsn = null;
    private  MajDataSourceName cassDsn = null;
    private  MajDataSourceName jdbcDsn = null;
    private  int minMinor  = 0;
    private  int minMajor = 0;

    private MajDataSourceName sourceName;

    public DbBeanWithFailOverInstance(DBEnvSetup dbEnvSetup, MajDataSource source, MajDataSourceName name){
        sourceName = null;
        this.source = source;
            cassandraState = new CassandraState(source.getVariant()==DatabaseVariant.CASSANDRA);
            entityFinder = new EntityFinder();
            this.envSetup = dbEnvSetup;
            factory = new MajoranaDBConnectionFactory(envSetup, new CassandraState(true));
            if (factory.getCassandraTemplate(name)==null){
                cassandraTemplate = mock(CassandraTemplate.class);
                CqlTemplate cqlTemplate = mock(CqlTemplate.class);
                cassandraIsMain =  new CassandraState(false);
            } else {
                cassandraTemplate = factory.getCassandraTemplate(name).orElse(null);
                CqlTemplate cqlTemplate = (CqlTemplate) cassandraTemplate.getCqlOperations();
                cassandraIsMain = new CassandraState(factory.getMainVariant() == DatabaseVariant.CASSANDRA);
            }
            jdbcTemplate = factory. getJdbcTemplate(name).orElse(null);
            namedTemplate  = factory.getNamedParameterJdbcTemplate(name).orElse(null);

            if (jdbcTemplate==null){
                LOGGER.warn("Jdbc DB connection "+envSetup. getMainDBName()+" not found");
            }
            cassandraState = cassandraIsMain;
            entityFinder = new EntityFinder();

            jdbcTemplate = factory. getJdbcTemplate(name).orElse(null);
            namedTemplate  = factory.getNamedParameterJdbcTemplate(name).orElse(null);

            if (jdbcTemplate==null){
                LOGGER.warn("Jdbc DB coD/Nnnection "+envSetup. getMainDBName()+" not found");
            }
        }

        public void setNext(DbBeanWithFailOverInstance i){
            this.next = i;
        }

    public boolean connect() throws ClassNotFoundException,SQLException{
        if (dbCon!=null){ return true; }
        try {
            if (envSetup==null){
                envSetup = new DBEnvSetup(new CassandraState(false), new HashMap<>());
            }
            //  Class.forName(dbDriver);
            MajDataSourceName smn = envSetup.getMainDBName();
            MajDataSourceName casn = envSetup.getMainCassDBName();
            MajDataSourceName jdsn = envSetup.getMainSqlDBName();

            factory = new MajoranaDBConnectionFactory(envSetup, new CassandraState(casn!=null));

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
        } catch ( SQLException e){
            LOGGER.error("Error connecting to db "+e);
            return false;
        }
    }




    public synchronized MajoranaAnnotationRepository getRepo(Class beanClass){
        MajoranaAnnotationRepository exist = classDBmap.get(beanClass);
        if (exist!=null){
            return exist;
        } else {
            MajoranaAnnotationRepository maj = new MajoranaAnnotationRepository(new MajoranaDBConnectionFactory(envSetup, cassandraIsMain),
                    sourceName,beanClass);
            classDBmap.put(beanClass, maj);
            return maj;
        }
    }


    @Override
    public Object getBeanNP( Class beanClass,  String table, String sql, String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);
//        String sql = mj.getR

        if (isCass) {
            try {
                PreparedStatement pres = cs.prepare(sql);
                BoundStatement bounds = pres.bind(params);
                return cassandraTemplate.selectOne(bounds, beanClass);
            } catch (Exception e) {
                if (next != null) {
                    return next.getBeanNP(beanClass, table, sql, paramNames, params);
                }
                LOGGER.warn("Error Executing cql in cassandra " + sql, e);
                throw e;
            }
        } else {
            Map<String, Object> mp = getParamMap(paramNames, params);
            SqlParameterSource sps = new MapSqlParameterSource(mp);

            try {
                return namedTemplate.queryForObject(sql, sps, mj.getMapper());
            } catch (Exception e) {
                if (next != null) {
                    return next.getBeanNP(beanClass, table, sql, paramNames, params);
                }
                    LOGGER.warn("Error Executing sql in jdbc template " + sql, e);
                    throw e;
            }

        }
    }

    public Object getBeanNPWithSelectClause( Class beanClass,  String table, String sql1, String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        String sql = mj. getReadStringNPSelectClause( table,  sql1, paramNames, params);
//        String sql = mj.getR

        if (isCass){
            try {
                PreparedStatement pres = cs.prepare(sql);
                BoundStatement bounds = pres.bind(params);
                return cassandraTemplate.selectOne(bounds, beanClass );
            } catch (Exception e){
                if (next!=null){ return next.getBeansNPWithSelectClause(beanClass, table, sql, paramNames, params);
            }
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            Map<String, Object> mp = getParamMap(paramNames, params);
            SqlParameterSource sps = new MapSqlParameterSource(mp);

            try {
                return namedTemplate.queryForObject(sql, sps, mj.getMapper());
            } catch (Exception e){
                if (next!=null){ return next.getBeansNPWithSelectClause(beanClass, table, sql, paramNames, params); }
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }





    public Object getBean( Class beanClass,  String table, String sql1, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        String sql = mj. getReadStringSelectClause( table, sql1);
//        String sql = mj.getR

        if (isCass){
            try {
                PreparedStatement pres = cs.prepare(sql);
                BoundStatement bounds = pres.bind(params);
                return cassandraTemplate.selectOne(bounds, beanClass );
            } catch (Exception e){
                if (next!=null){ return next.getBean(beanClass, table, sql, params); }
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                return jdbcTemplate.queryForObject(sql, params, mj.getMapper());
            } catch (Exception e){
                if (next!=null){ return next.getBean(beanClass, table, sql, params); }

                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }

    public List<Object> getBeans(Class beanClass, String table, String sql1, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);

        String sql = mj. getReadStringSelectClause( table, sql1);
//        String sql = mj.getR

        if (isCass){
            try {
                PreparedStatement pres = cs.prepare(sql);
                BoundStatement bounds = pres.bind(params);
                return cassandraTemplate.select(bounds, beanClass );
            } catch (Exception e){
                if (next !=null){ return next.getBeans( beanClass, table, sql,  params); }
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                return jdbcTemplate.query(sql, params, mj.getMapper());
            } catch (Exception e){
                if (next !=null){ return next.getBeans( beanClass, table, sql,  params); }
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }

    public Object[] getBeansArrayNP(Class beanClass, String table, String sql, String[] paramNames, Object[] params) throws SQLException {
        List<Object> obj = getBeansNP(beanClass, table, sql, paramNames, params);
        return obj.stream().toArray();
    }

    public Object[] getBeansArray(Class beanClass, String table, String sql,  Object[] params) throws SQLException {
        List<Object> obj = getBeans(beanClass, table, sql,  params);
        return obj.stream().toArray();
    }



    public Map<String, Object> getParamMap(String named[], Object params[]){
        HashMap<String, Object> ret = new HashMap<>();
        int len= Math.min(named.length, params.length);
        for(int i=0; i<len; i++){
            ret.put(named[i], params[i]);
        }
        return ret;
    }

    public List<Object> getBeansNPWithSelectClause( Class beanClass,  String table, String sql1,  String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);


        String sql = mj.getReadStringNPSelectClause( table, sql1, paramNames, params);
//        String sql = mj.getR

        if (isCass){
            try {
                PreparedStatement pres = cs.prepare(sql);
                BoundStatement bounds = pres.bind(params);
                return cassandraTemplate.select(bounds, beanClass );
            } catch (Exception e){
                if (next != null){ return next.getBeansNP( beanClass, table, sql,paramNames, params); }
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                MapSqlParameterSource src = new MapSqlParameterSource(getParamMap(paramNames, params));
                return namedTemplate.query(sql, src, mj.getMapper());
            } catch (Exception e){
                if (next != null){ return next.getBeansNP( beanClass, table, sql, paramNames, params); }
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }

    public List<Object> getBeansNP( Class beanClass,  String table, String sql,  String[] paramNames, Object[] params) throws SQLException {

        MajoranaAnnotationRepository mj = getRepo(beanClass);
//        String sql = mj.getR

        if (isCass){
            try {
                PreparedStatement pres = cs.prepare(sql);
                BoundStatement bounds = pres.bind(params);
                return cassandraTemplate.select(bounds, beanClass );
            } catch (Exception e){
                if (next != null) { return next.getBeansNP( beanClass, table, sql, paramNames,  params); }
                LOGGER.warn("Error Executing cql in cassandra "+sql,e);
                throw e;
            }
        } else {
            try {
                MapSqlParameterSource src = new MapSqlParameterSource(getParamMap(paramNames, params));
                return namedTemplate.query(sql, src, mj.getMapper());
            } catch (Exception e){
                if (next != null) { return next.getBeansNP( beanClass, table, sql, paramNames, params); }
                LOGGER.warn("Error Executing sql in jdbc template "+sql,e);
                throw e;
            }
        }

    }

    public List<Object> getBeansNPWithSelectClause(Class<? extends BaseMajoranaEntity> beanClass, String sql1, String[] paramNames, Object[] params) throws SQLException {
        return this.getBeansNPWithSelectClause( beanClass, getTableName(beanClass) ,sql1, paramNames, params);
    }
    public List<Object> getBeans( Class<? extends BaseMajoranaEntity> beanClass, String sql1,  Object[] params) throws SQLException {
        return this.getBeans( beanClass, getTableName(beanClass),sql1, params);
    }
    public Object[] getBeansArrayNP(Class<? extends BaseMajoranaEntity> beanClass, String sql,  String[] paramNames, Object[] params) throws SQLException {
        return this.getBeansNP(beanClass, getTableName(beanClass), sql, paramNames, params).toArray(new Object[]{});
    }
    public Object[] getBeansArrayNPWithSelectClause(Class<? extends BaseMajoranaEntity> beanClass, String sql,  String[] paramNames, Object[] params) throws SQLException {
        return this.getBeansNPWithSelectClause(beanClass, getTableName(beanClass), sql, paramNames, params).toArray(new Object[]{});
    }
    private String getTableName(Class<? extends BaseMajoranaEntity> beanClass){
        try {
            BaseMajoranaEntity inst = beanClass.newInstance();
            return inst.getTableName();
        } catch (InstantiationException | IllegalAccessException e){
            LOGGER.error("Cannot Instantiate "+beanClass.getCanonicalName());
        }
        return "-- NO TABLE FOUND --";
    }

    public Object[] getBeansArray(Class<? extends BaseMajoranaEntity> beanClass, String sql,  Object[] params) throws SQLException {
        return this.getBeans(beanClass, getTableName(beanClass), sql,  params).toArray(new Object[]{});
    }

    public Object getBean( Class<? extends BaseMajoranaEntity> beanClass,  String sql, Object[] params) throws SQLException {
        List<Object> a = this.getBeans(beanClass, getTableName(beanClass), sql,  params);
        if (a.size()>0){ return a.get(0); }
        return null;
    }

    public List<Object> getBeansNP(Class<? extends BaseMajoranaEntity> beanClass, String sql, String[] paramNames, Object[] params) throws SQLException{
        return this.getBeansNP(beanClass, getTableName(beanClass), sql, paramNames, params);
    }

    public Object getBeanNP( Class<? extends BaseMajoranaEntity> beanClass,  String sql, String[] paramNames, Object[] params) throws SQLException {
        List<Object> a = this.getBeansNP(beanClass, getTableName(beanClass), sql, paramNames, params);
        if (a.size()>0){ return a.get(0); }
        return null;
    }

}


