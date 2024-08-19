package Distiller.DBs;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.model.SimpleTypeHolder;

import java.util.HashSet;
import java.util.LinkedList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Spring configuration, mocks up a empty cassandra connection if
 * the spring profile "mock-cass" is active
 */

@Configuration
@Profile(value="mock-cass")
public class CassandraMock {


    @Bean
    public CassandraState cassandraState() {
        return new CassandraState(false);
    }


    @Bean
    public CqlSession cassandraSession() {
        CqlSession sk = mock(CqlSession.class);
        when(sk.getContext()).thenReturn(mock(DriverContext.class));
        return sk;
    }

    @Bean
    public CassandraConverter cassandraConverter() {

        SimpleTypeHolder holder = new SimpleTypeHolder(new HashSet<>(), true);


        CustomConversions.StoreConversions storeConversions = CustomConversions.StoreConversions.of(holder, new LinkedList<>());

        CustomConversions conv = new CustomConversions(storeConversions, new LinkedList<>());

        CassandraConverter  cc = mock(CassandraConverter.class);
                org.mockito.Mockito.when(cc.getCustomConversions()).thenReturn(conv);

   /*         @NonNull
            @Override
            public <JavaTypeT> TypeCodec<JavaTypeT> codecFor(@NonNull DataType cqlType, @NonNull GenericType<JavaTypeT> javaType) {
                return null;
            }

            @NonNull
            @Override
            public <JavaTypeT> TypeCodec<JavaTypeT> codecFor(@NonNull DataType cqlType) {
                return null;
            }

            @NonNull
            @Override
            public <JavaTypeT> TypeCodec<JavaTypeT> codecFor(@NonNull GenericType<JavaTypeT> javaType) {
                return null;
            }

            @NonNull
            @Override
            public <JavaTypeT> TypeCodec<JavaTypeT> codecFor(@NonNull DataType cqlType, @NonNull JavaTypeT value) {
                return null;
            }

            @NonNull
            @Override
            public <JavaTypeT> TypeCodec<JavaTypeT> codecFor(@NonNull JavaTypeT value) {
                return null;
            }*/

        return cc;
    }



}
