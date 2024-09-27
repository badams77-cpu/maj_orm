package com.majorana.maj_orm.DBs;

import com.majorana.maj_orm.persist.newannot.TimeResult;
import com.majorana.maj_orm.ORM.MajoranaDBConnectionFactory;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
//import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 *  Used with Spring DB Health Indicators, to show weather the database connections are active
 */
@Service
public class MajDataSourceHealthIndicator extends AbstractHealthIndicator {


    private static final Logger LOGGER = LoggerFactory.getLogger(MajDataSourceHealthIndicator.class);

    @Autowired
    private DBEnvSetup dbEnvSetup;
    
    @Autowired
    private MajoranaDBConnectionFactory dbFactory;

    private MajDataSourceName datasourceName;

    private MajDataSourceHealthIndicator(MajDataSourceName datasourceName) {
        this.datasourceName = datasourceName;
    }

    @Override
    public void doHealthCheck(Health.Builder builder) throws Exception {
        String sql = "select 1 as status;";
        String down = "";
        DatabaseVariant variant = dbEnvSetup.getCreds(datasourceName).getVariant();
        HikariDataSource src = dbEnvSetup.getMajDataSource(datasourceName).getHikariDataSource();

   //     Optional<CassandraTemplate> cassTempOpt = dbFactory.getCassandraTemplate(datasourceName);
        Optional<JdbcTemplate> jdbcTemplate = dbFactory.getJdbcTemplate(datasourceName);
        
        boolean status = variant==DatabaseVariant.CASSANDRA ? false : false;
  //              cassTempOpt.map( template -> template.select(sql, TimeResult.class).stream().collect(Collectors.toList())).orElse(new LinkedList<>()).contains(Boolean.TRUE)
  //              : jdbcTemplate.map( template -> template.query(sql, new HealthMapper()).stream().collect(Collectors.toList())).orElse(new LinkedList<>()).contains(Boolean.TRUE);
        if (!status){
            down+=datasourceName.getDataSourceName()+" Down";
        }
 //       if (status){
 //           status = dbFactory.getJdbcTemplate(PortalEnum.DC).map( template -> template.query(sql, new HealthMapper()).stream().collect(Collectors.toList())).orElse(new LinkedList<>()).contains(Boolean.TRUE);
 //           down+=down.isEmpty()?"":", ";
 //           down+="DC Down";
 //       }
        if (status){
            builder.up();
        } else {
            LOGGER.warn("doHealthCheck: Database connection down: "+down);
            builder.down();
        }
    }

    public class HealthMapper implements  RowMapper<Boolean> {
        public Boolean mapRow(ResultSet rs, int i) throws SQLException {
            return rs.getBoolean(1);
        }
    }
}
