package Distiller.ORM;

import Distiller.DBs.*;
import Distiller.TimeResult;
import Distiller.Utils.MethodPrefixingLoggerFactory
        ;
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


public class MajoranaDBConnectionFactory {

        private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(MajoranaDBConnectionFactory.class);

        private DBEnvSetup dBSourcesFromEnv;

        private CassandraTemplate mockCass;

        private CassandraState cassandraState;


        public MajoranaDBConnectionFactory(DBEnvSetup dbs, CassandraState cassandraState) {
                  dBSourcesFromEnv = dbs;
            mockCass = mock(CassandraTemplate.class);
            this.cassandraState = cassandraState;
        }

        public SmokDatasourceName getMainDBName(){
            SmokDatasourceName dbName =  dBSourcesFromEnv.getMainSqlDBName();
            return dbName;
//            return cassandraState.isEnabbled() ? dBSourcesFromEnv.getMainSqlDBName() :
//                    dBSourcesFromEnv.getMainSqlDBName();
        }

        public DatabaseVariant getMainVariant(){
            SmokDatasourceName mainVariant = getMainDBName();
            DBCreds mCreds = dBSourcesFromEnv.getCreds(mainVariant);
            if (mCreds == null){

                LOGGER.warn("No main DB creds found");
            }
            DatabaseVariant dv = mCreds.getVariant();
//            mainIsCass = dv==DatabaseVariant.CASSANDRA;
            return dv;
        }



        public String getSchemaInDB(SmokDatasourceName dbSrcName){
            DBCreds creds = dBSourcesFromEnv.getCreds(dbSrcName);
            if (creds==null){
                LOGGER.warn("No schema found for src name "+dbSrcName);
                return "";
            }
            return creds.getGroup();
        }

        public Optional<JdbcTemplate> getMainJdbcTemplate(){
            SmokDatasourceName sdn = (dBSourcesFromEnv. getMainSqlDBName());
            DataSource daSrc = (dBSourcesFromEnv.getHikDatasource(sdn));
            Optional<JdbcTemplate> jdbc = Optional.ofNullable(  daSrc == null ? null :
                    new JdbcTemplate( daSrc ));
            if (!jdbc.isPresent()){
                LOGGER.warn("No jdbc found for src name "+sdn);
            }
            return jdbc;
        }

    public Optional<NamedParameterJdbcTemplate> getMainNamedParamJdbcTemplate(){
        SmokDatasourceName sdn = (dBSourcesFromEnv. getMainSqlDBName());
        DataSource daSrc = (dBSourcesFromEnv.getHikDatasource(sdn));
        Optional<NamedParameterJdbcTemplate> jdbc = Optional.ofNullable(  daSrc == null ? null :
                new NamedParameterJdbcTemplate( daSrc ));
        if (!jdbc.isPresent()){
            LOGGER.warn("No jdbc found for src name "+sdn);
        }
        return jdbc;
    }

        public Optional<CassandraTemplate> getMainCassandraTemplate(){
            SmokDatasourceName sdsn =dBSourcesFromEnv.getMainCassDBName();
            if (sdsn == null){
                LOGGER.warn("No Cass Datasource using mock");
                return Optional.of(mockCass);
            }
            Optional<CassandraTemplate> ct = getCassandraTemplate(
                    dBSourcesFromEnv. getMainCassDBName()
            );
            return ct;
        }

        public Optional<JdbcTemplate> getJdbcTemplate(SmokDatasourceName dbSrcName) {
            HikariDataSource source = dBSourcesFromEnv.getHikDatasource(dbSrcName);
            if (source==null){
                LOGGER.warn("No datasource found for src name "+dbSrcName);
                return Optional.empty();
            }
            return Optional.of(new JdbcTemplate(source));
        }

        public Connection getMysqlConn(SmokDatasourceName dbSrcName) throws SQLException {
            HikariDataSource source = dBSourcesFromEnv.getHikDatasource(dbSrcName);
            if (source==null){
                LOGGER.warn("No datasource found for src name "+dbSrcName);
                return null;
            }
            return source.getConnection();
        }

        public DatabaseVariant getVariant(SmokDatasourceName dbName){
            return dBSourcesFromEnv.getCreds(dbName).getVariant();
        }


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

        public LocalDateTime getDBTime(SmokDatasourceName dbName){
            DatabaseVariant dbVariant = dBSourcesFromEnv.getSmokDatasource(dbName).getVariant();

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

        public Optional<CqlSession> getCqlSession(SmokDatasourceName dbName) {
            CqlSession cSess = dBSourcesFromEnv.getCqlSession(dbName);
            return Optional.of(cSess);
        }

        public Optional<CassandraTemplate> getCassandraTemplate(SmokDatasourceName dbName){
            CqlSession cSess = dBSourcesFromEnv.getCqlSession(dbName);
            com.datastax.driver.core.Session session =  (com.datastax.driver.core.Session) cSess;
            CassandraTemplate cass = cSess == null ? mockCass: new  CassandraTemplate( session);
            return Optional.of(cass);
        }


        public Optional<NamedParameterJdbcTemplate> getNamedParameterJdbcTemplate(SmokDatasourceName dbName) {
            HikariDataSource hds = dBSourcesFromEnv.getHikDatasource(dbName);    ;
            return Optional.of(new NamedParameterJdbcTemplate(hds));
        }


        public class TimeMapper implements RowMapper<LocalDateTime> {
            @Override
                public LocalDateTime mapRow(ResultSet rs, int i) throws SQLException {
                    java.sql.Timestamp ts = rs.getTimestamp("dbtime");
                    return ts.toLocalDateTime();
                }
            }
}
