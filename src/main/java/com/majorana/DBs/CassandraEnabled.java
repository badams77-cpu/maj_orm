package Majorana.DBs;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * CassandraEnabled
 *
 * A spring style run time bean that passes the cassandra state according to weather the
 * active spring profiles include "mock-cass" or not
 */


@Configuration
@Profile(value="!mock-cass")
public class CassandraEnabled {

    @Bean
    public CassandraState cassandraState() {
        return new CassandraState(true);
    }

}
