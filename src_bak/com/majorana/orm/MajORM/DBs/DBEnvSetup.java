package com.majorana.orm.MajORM.DBs;


//import com.datastax.oss.driver.api.core.CqlIdentifier;
//import com.datastax.oss.driver.api.core.CqlSession;
//import com.smokpromotion.SmokProm.config.common.YamlDBConfig;
//import CassandraConnector;
//import SmokDataSource;
//import SmokDatasourceName;

import com.datastax.oss.driver.api.core.CqlSession;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import com.majorana.orm.MajORM.Utils.MethodPrefixingLoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DBEnvSetup {

    private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(DBEnvSetup.class);

    private final static String[] CredFields = DBCreds.getCredFields();

    private final static String PREFIX = "DBE";

    private final static String SEPERATOR = "_";

//    private static final String PREFIX = YamlDBConfig.getPrefix()

//    @Autowired
//    private Environment env;

//    @Autowired
//    private YamlDBConfig ydb;

    private List<DBCreds> envCredList;

    private  Map<MajDatasourceName, DBCreds> envCredMap = new HashMap<>();

    private  Map<MajDatasourceName, HikariDataSource> dataSources  = new HashMap<>();

    private  Map<MajDatasourceName, MajDataSource> smokDataSourceMap  = new HashMap<>();

    private  Map<MajDatasourceName, CqlSession> smokCassMap  = new HashMap<>();

    public static Map<String, String> getAllKnownProperties() {
        Map<String, String> rtn = System.getenv();
        return rtn;
    }

    public static <T> List<T> withoutFirst(List<T> o) {
        final List<T> result = new ArrayList<T>();
        for (int i = 1; i < o.size(); i++)
            result.add(o.get(i));

        return result;
    }

    public DBEnvSetup(){

        Map<String, String> allProp = getAllKnownProperties();



        Map<List<String>, String> splitKeys = allProp.entrySet().stream()
                .collect( Collectors.toMap(
                        x->Arrays.asList( ((String) x.getKey()).split(SEPERATOR)),
                        Map.Entry::getValue,
                        (a,b)->a)
                );

        Map<List<String>, Object> withPrefix = splitKeys.entrySet().stream()
                .filter( x->x.getKey().size()>1 && x.getKey().get(0).equalsIgnoreCase(PREFIX) )
                .collect( Collectors.toMap(x->withoutFirst(x.getKey()), Map.Entry::getValue));

        Map<String, Map<String, String>> propsByDb = withPrefix.entrySet().stream()
                .filter( x->x.getKey().size()>1 )
                .collect( Collectors.groupingBy( x-> ((Map.Entry<List<String>, Object>) x).getKey().get(0),
                                Collectors.toMap( y-> String.join(SEPERATOR,
                                                withoutFirst(((Map.Entry<List<String>, Object>) y).getKey())),
                                        z->((Map.Entry<List<String>, Object>) z).getValue().toString())
                        )
                );

        envCredMap = new HashMap<>();
        dataSources = new HashMap<>();
        smokDataSourceMap = new HashMap<>();
//        smokCassMap = new HashMap<>();
        envCredList = new LinkedList<>();
        Map<String, Object> map = new HashMap<>();
        for(Iterator<Map.Entry<String, Map<String, String>>> it = propsByDb.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Map<String, String>> entry = it.next();
            String topKey = entry.getKey();
            Map<String, String> propertySource = entry.getValue();
            for (String key : propertySource.keySet()) {
                String longKey = PREFIX + SEPERATOR+ topKey + SEPERATOR + key;
                String envValue = allProp.get(longKey);
                String val = envValue==null ? propertySource.get(key) : envValue;
                map.put(longKey, val);
            }
        }
        LOGGER.warn("Read "+map.size()+" env and property items");

        Map<String, Object> dbEnvMap = allProp.entrySet().stream().filter( en->en.getKey().startsWith(PREFIX+SEPERATOR)).collect(Collectors.toMap(en->en.getKey().substring(PREFIX.length()+1),en->en.getValue()));

        Set<String> names =  dbEnvMap.keySet().stream().map(x->x.substring(0,x.indexOf(SEPERATOR))).collect(Collectors.toSet());

        LOGGER.warn("Read "+names.size()+" DBs to connect to");

        for(String smkDS : names){
            MajDatasourceName sdsn = new MajDatasourceName(smkDS);
            Map<String, Object> credData =
                    dbEnvMap.entrySet().stream().filter( en->en.getKey().startsWith(sdsn.getDataSourceName()))
                            .collect(Collectors.toMap( x->x.getKey(), y->y.getValue()));
            int found = 0;

            LOGGER.warn("Seting up source "+sdsn);

            Map<String, String> credMap = new HashMap<>();
            String stem = PREFIX+SEPERATOR+smkDS+SEPERATOR;

            for(String field : CredFields) {
                String key = smkDS +SEPERATOR+ field;
                Object oVal = credData.get(key);

                String value = "";
                if (oVal == null) {
                    LOGGER.warn("Missing DB Set Env Var: " + key);
                    continue;
                } else {
                    if (oVal instanceof Number ) {
                        long n = ((Number) oVal).longValue();
                        value = "" + n;
                    } else if (oVal instanceof Boolean) {
                        value = ((Boolean) oVal).toString();
                    } else {
                        value = (String) credData.get(key);
                    }
                }
                found++;
                credMap.put(field, value);
            }
            if (found >= CredFields.length) {
                try {
                    DBCreds creds = new DBCreds(credMap);
                    envCredMap.put(sdsn, creds);
                    envCredList.add(creds);
                    MajDataSource src = new MajDataSource(creds);
                    smokDataSourceMap.put(sdsn, src);
                    if (creds.getVariant() == com.majorana.orm.MajORM.DBs.DatabaseVariant.CASSANDRA) {
                        CassandraConnector cassConn = new CassandraConnector();
                        cassConn.connect(creds);

                        //    CqlSession cqlSess = cassConn.getSession();
                        //    if (cqlSess==null){
                        //        LOGGER.error("Error get CqlSession "+sdsn+" "+cassConn.getMessage());
                        //        continue;
                        //    }
                        //    cqlSess.execute("USE " + CqlIdentifier.fromCql(creds.getName().getDataSourceName()));
                        ;
                        //    smokCassMap.put(sdsn, cqlSess);
                    } else {
                        HikariDataSource hikariDataSource = src.getHikariDataSource();
                        dataSources.put(sdsn, hikariDataSource);
                    }
                } catch (Exception e){
                    LOGGER.error("Error create datasource "+sdsn,e);
                }
            } else {
                String missing = Arrays.asList(CredFields).stream().map( k-> smkDS+SEPERATOR+k)
                        .filter( k->credMap.containsKey(k)).collect(Collectors.joining(", "));
                LOGGER.error("DB Config: " + sdsn + " missing fields "+missing);
            }
        }
    }

    public DBCreds getCreds(MajDatasourceName dbName){
        return envCredMap.getOrDefault(dbName, new DBCreds(new HashMap<>()));
    }

    public HikariDataSource getHikDatasource(MajDatasourceName dbSrcName)
    {
        return dataSources.get(dbSrcName);
    }

    public MajDataSource getSmokDatasource(MajDatasourceName dbSrcName){
        return smokDataSourceMap.get(dbSrcName);
    }

    public CqlSession getCqlSession(MajDatasourceName dbSrcName){
        return smokCassMap.get(dbSrcName);
    }

    public MajDatasourceName getMainCassDBName(){
        return envCredList.stream().filter(x-> x.getVariant()== com.majorana.orm.MajORM.DBs.DatabaseVariant.CASSANDRA)
                .findFirst().orElse(new DBCreds()).getName()
                ;
    }

    public MajDatasourceName getMainSqlDBName(){
        return envCredList.stream().filter(x->x.getVariant() != DatabaseVariant.CASSANDRA)
                .findFirst().orElse(new DBCreds()).getName()
                ;
    }

    public MajDatasourceName getMainDBName(){
        return envCredList.stream()
                .findFirst().orElse(new DBCreds()).getName()
                ;
    }
}
