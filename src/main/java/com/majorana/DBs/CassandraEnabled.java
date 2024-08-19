package Distiller.DBs;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile(value="!mock-cass")
public class CassandraEnabled {

    @Bean
    public CassandraState cassandraState() {
        return new CassandraState(true);
    }

}
