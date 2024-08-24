package com.majorana.ORM;

import com.majorana.Utils.MethodPrefixingLoggerFactory;
import com.majorana.DBs.*        ;
import com.datastax.oss.driver.api.core.CqlSession;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;


import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.Optional;

import static org.mockito.Mockito.mock;

/***
 * This class represents a factory to create database connections using any of the credentials
 * in the DBEnvSetup class of database from the environment
 */

public class MajoranaDBConnectionFactory {

        private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(MajoranaDBConnectionFactory.class);

        private DBEnvSetup dBSourcesFromEnv;

        private CassandraTemplate mockCass;

        private CassandraState cassandraState;


    /**
     * Constructor, make a connection factory from the DB Environment, and with
     * cassandra option
     *
     * @param dbs  - DBEnvSetup env var for the credentials
     * @param cassandraState - is cassandra to to be used
     */


    public MajoranaDBConnectionFactory(DBEnvSetup dbs, CassandraState cassandraState) {
                  dBSourcesFromEnv = dbs;
            mockCass = mock(CassandraTemplate.class);
            this.cassandraState = cassandraState;
        }

    /**
     *
     * @return get the name of the main database
     */


    public MajDatasourceName getMainDBName(){
            MajDatasourceName dbName =  dBSourcesFromEnv.getMainSqlDBName();
            return dbName;
//            return cassandraState.isEnabbled() ? dBSourcesFromEnv.getMainSqlDBName() :
//                    dBSourcesFromEnv.getMainSqlDBName();
        }

    /**
     * Get the variant of the main database
     *
     * @return
     */

    public DatabaseVariant getMainVariant(){
            MajDatasourceName mainVariant = getMainDBName();
            DBCreds mCreds = dBSourcesFromEnv.getCreds(mainVariant);
            if (mCreds == null){

                LOGGER.warn("No main DB creds found");
            }
            DatabaseVariant dv = mCreds.getVariant();
//            mainIsCass = dv==DatabaseVariant.CASSANDRA;
            return dv;
        }

    /**
     * Get the group schema from the MajDatasourceName
     *
     *
     * @param dbSrcName
     * @return group from credentials from the source
     */

    public String getSchemaInDB(MajDatasourceName dbSrcName){
            DBCreds creds = dBSourcesFromEnv.getCreds(dbSrcName);
            if (creds==null){
                LOGGER.warn("No schema found for src name "+dbSrcName);
                return "";
            }
            return creds.getGroup();
        }

    /**
     * Get trhe main jdbc template
     *
     * @return an JbdcTemplate
     */

    public Optional<JdbcTemplate> getMainJdbcTemplate(){
            MajDatasourceName sdn = (dBSourcesFromEnv. getMainSqlDBName());
            DataSource daSrc = (dBSourcesFromEnv.getHikDatasource(sdn));
            Optional<JdbcTemplate> jdbc = Optional.ofNullable(  daSrc == null ? null :
                    new JdbcTemplate( daSrc ));
            if (!jdbc.isPresent()){
                LOGGER.warn("No jdbc found for src name "+sdn);
            }
            return jdbc;
        }

    /**
     * Get a named parameter template for the main template
     *
     * @return named parameter jdbc template
     */

    public Optional<NamedParameterJdbcTemplate> getMainNamedParamJdbcTemplate(){
        MajDatasourceName sdn = (dBSourcesFromEnv. getMainSqlDBName());
        DataSource daSrc = (dBSourcesFromEnv.getHikDatasource(sdn));
        Optional<NamedParameterJdbcTemplate> jdbc = Optional.ofNullable(  daSrc == null ? null :
                new NamedParameterJdbcTemplate( daSrc ));
        if (!jdbc.isPresent()){
            LOGGER.warn("No jdbc found for src name "+sdn);
        }
        return jdbc;
    }

    /**
     * Get the main cassandra template
     * 
     * @return cassandra template
     */
    
        public Optional<CassandraTemplate> getMainCassandraTemplate(){
            MajDatasourceName sdsn =dBSourcesFromEnv.getMainCassDBName();
            if (sdsn == null){
                LOGGER.warn("No Cass Datasource using mock");
                return Optional.of(mockCass);
            }
            Optional<CassandraTemplate> ct = getCassandraTemplate(
                    dBSourcesFromEnv. getMainCassDBName()
            );
            return ct;
        }

    /**
     * Get the a jdnc template for the named database source
     * 
     * 
     * @param dbSrcName
     * @return
     */

    public Optional<JdbcTemplate> getJdbcTemplate(MajDatasourceName dbSrcName) {
            HikariDataSource source = dBSourcesFromEnv.getHikDatasource(dbSrcName);
            if (source==null){
                LOGGER.warn("No datasource found for src name "+dbSrcName);
                return Optional.empty();
            }
            return Optional.of(new JdbcTemplate(source));
        }

    /**
     * Get an SQL Connection for the named datasource
     * 
     * 
     * @param dbSrcName
     * @return SQL Connection
     * @throws SQLException
     */

    public Connection getMysqlConn(MajDatasourceName dbSrcName) throws SQLException {
            HikariDataSource source = dBSourcesFromEnv.getHikDatasource(dbSrcName);
            if (source==null){
                LOGGER.warn("No datasource found for src name "+dbSrcName);
                return null;
            }
            return source.getConnection();
        }

    /**
     * Get the variant of the named datasource
     * 
     * @param dbName
     * @return
     */

    public DatabaseVariant getVariant(MajDatasourceName dbName){
            return dBSourcesFromEnv.getCreds(dbName).getVariant();
        }


    /**
     * 
     * Translate SQL according to the database variant
     * 
     * 
     * 
     * @param sql in
     * @param dbVariant
     * @return sql out
     */
        
        public String translate(String sql, DatabaseVariant dbVariant){
                String translatedSQL = sql;
                if (DatabaseVariant.SQL_SERVER == dbVariant) {
                    translatedSQL = sql.replaceAll("now\\(\\)", "SYSDATETIME\\(\\)")
                            .replaceAll("length\\(", "LEN\\(")
                            .replaceAll("ifnull\\(", "isnull(")
                            // SQL server uses TOP instead of LIMIT
                            .replaceAll("(?i)\\{limit1_pre\\}", "TOP 1")
                            .replaceAll("(?i)\\{limit1_post\\}", "");
                } else if (DatabaseVariant.MYSQL == dbVariant) {
                    translatedSQL = sql
                            // MySQL server uses LIMIT
                            .replaceAll("(?i)\\{limit1_pre\\}", "")
                            .replaceAll("(?i)\\{limit1_post\\}", "LIMIT 1")
                    ;
                } else if (DatabaseVariant.CASSANDRA == dbVariant) {
                    translatedSQL = sql;
                }
                return translatedSQL;

        }

    /**
     *  Get the database time zone given the database name
     * 
     * @param dbName
     * @return
     */

    public LocalDateTime getDBTime(MajDatasourceName dbName){
            DatabaseVariant dbVariant = dBSourcesFromEnv.getMajDatasource(dbName).getVariant();

            switch(dbVariant){
                case MYSQL:
                    // use now(3) to get time with microsecond precision
                    return getJdbcTemplate(dbName).map( template->template.query("select now(3) as dbtime", new TimeMapper())).orElse(new LinkedList<LocalDateTime>()).stream().findFirst().orElse(null);
                case SQL_SERVER:
                    return getJdbcTemplate(dbName).map( template->template.query("select SYSDATETIME() as dbtime", new TimeMapper())).orElse(new LinkedList<LocalDateTime>()).stream().findFirst().orElse(null);
                case CASSANDRA:
                    return getCassandraTemplate(dbName).map( template->template.select("select toTimestamp(now() as dbtime)", TimeResult.class))
                            .orElse(new LinkedList<>())
                            .stream().findFirst().orElse(new TimeResult()).getDatetime();
                default:
                    return null;
            }
        }

    /**
     * Get a CqlSession (cassandra) for the named datab source
     * 
     * @param dbName
     * @return
     */

    public Optional<CqlSession> getCqlSession(MajDatasourceName dbName) {
            CqlSession cSess = dBSourcesFromEnv.getCqlSession(dbName);
            return Optional.of(cSess);
        }

    /**
     * Get a cassandra template for the named database source
     * 
     * @param dbName
     * @return
     */

        public Optional<CassandraTemplate> getCassandraTemplate(MajDatasourceName dbName){
            CqlSession cSess = dBSourcesFromEnv.getCqlSession(dbName);
            com.datastax.driver.core.Session session =  (com.datastax.driver.core.Session) cSess;
            CassandraTemplate cass = cSess == null ? mockCass: new  CassandraTemplate( session);
            return Optional.of(cass);
        }

    /**
     * Get a named parameter jdnc template for the named datasource
     * 
     * @param dbName
     * @return
     */

        public Optional<NamedParameterJdbcTemplate> getNamedParameterJdbcTemplate(MajDatasourceName dbName) {
            HikariDataSource hds = dBSourcesFromEnv.getHikDatasource(dbName);    ;
            return Optional.of(new NamedParameterJdbcTemplate(hds));
        }

    /**
     *  Get a row mapper ot get a localdate time from a database
     */

    public class TimeMapper implements RowMapper<LocalDateTime> {
            @Override
                public LocalDateTime mapRow(ResultSet rs, int i) throws SQLException {
                    java.sql.Timestamp ts = rs.getTimestamp("dbtime");
                    return ts.toLocalDateTime();
                }
            }
}
