package com.majorana.orm.MajORM.ORM;

import com.majorana.orm.MajORM.DBs.DBCreds;
import com.majorana.orm.MajORM.DBs.MajDatasourceName;
import com.datastax.oss.driver.api.core.CqlSession;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import com.majorana.orm.MajORM.DBs.DBEnvSetup;
import com.majorana.orm.MajORM.DBs.DatabaseVariant;
import com.majorana.orm.MajORM.ORM.TimeResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.Optional;

import static org.mockito.Mockito.mock;


public class MajoranaDBConnectionFactory {

        private DBEnvSetup dBSourcesFromEnv;

        private CassandraTemplate mockCass;

        private CassandraState cassandraState;

        public MajoranaDBConnectionFactory(DBEnvSetup dbs, CassandraState cassandraState) {
                  dBSourcesFromEnv = dbs;
            mockCass = mock(CassandraTemplate.class);
            this.cassandraState = cassandraState;
        }

        public MajDatasourceName getMainDBName(){
            return cassandraState.isEnabled() ? dBSourcesFromEnv.getMainSqlDBName() :
                    dBSourcesFromEnv.getMainSqlDBName();
        }

        public String getSchemaInDB(MajDatasourceName dbSrcName){
            DBCreds creds = dBSourcesFromEnv.getCreds(dbSrcName);
            if (creds==null) return "";
            return creds.getGroup();
        }

        public Optional<JdbcTemplate> getJdbcTemplate(MajDatasourceName dbSrcName) {
            HikariDataSource source = dBSourcesFromEnv.getHikDatasource(dbSrcName);
            if (source==null){ return Optional.empty(); }
            return Optional.of(new JdbcTemplate(source));
        }

        public Connection getMysqlConn(MajDatasourceName dbSrcName) throws SQLException {
            HikariDataSource source = dBSourcesFromEnv.getHikDatasource(dbSrcName);
            return source.getConnection();
        }

        public DatabaseVariant getVariant(MajDatasourceName dbName){
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
                    translatedSQL = sql

                    ;
                }
                return translatedSQL;

        }

        public LocalDateTime getDBTime(MajDatasourceName dbName){
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

        public Optional<CassandraTemplate> getCassandraTemplate(MajDatasourceName dbName){
            CqlSession cSess = dBSourcesFromEnv.getCqlSession(dbName);
            CassandraTemplate cass = cSess == null ? mockCass: new  CassandraTemplate(cSess);
            return Optional.of(cass);
        }


        public Optional<NamedParameterJdbcTemplate> getNamedParameterJdbcTemplate(MajDatasourceName dbName) {
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
