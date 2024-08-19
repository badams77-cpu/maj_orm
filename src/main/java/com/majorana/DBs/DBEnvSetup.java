package Distiller.DBs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import Distiller.Utils.MethodPrefixingLoggerFactory;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
/*
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
 */
import java.util.*;
import java.util.stream.Collectors;

public class DBEnvSetup {

    private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(DBEnvSetup.class);

    private final static String[] CredFields = DBCreds.getCredFields();

    private final static String PREFIX = "Distiller";

    private final static String DB_VERSION = "DBVersion";

    private final static String MAJOR = "Major";

    private final static String MINOR = "Minor";

    private final static String CHECK_DBVERSION = "CheckDbVersion";

    private final static String SEPERATOR = "_";

//    private static final String PREFIX = YamlDBConfig.getPrefix();

//    @Autowireds
//    private Environment env;

//    @Autowired
//    private YamlDBConfig ydb;

    private List<DBCreds> envCredList;

    private int dbVersionMinor;
    private int dbVersionMajor;
    private boolean checkDbVersion;

    private CassandraState cassandraState;

    private  Map<SmokDatasourceName, DBCreds> envCredMap;

    private  Map<SmokDatasourceName, HikariDataSource> dataSources;

    private  Map<SmokDatasourceName, SmokDataSource> smokDataSourceMap;

    private  Map<SmokDatasourceName, CqlSession> smokCassMap;

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


    public DBEnvSetup(CassandraState cassandraState, Map<String, String> addProp){

        this.cassandraState = cassandraState;

        Map<String, String> allProp = getAllKnownProperties();

        LOGGER.warn("DBEnvSetup: read "+allProp.size()+" all env vars");

        Map<List<String>, String> splitKeys = allProp.entrySet().stream()
                .collect( Collectors.toMap(
                    x->Arrays.asList( ((String) x.getKey()).split(SEPERATOR)),
                        Map.Entry::getValue,
                   (a,b)->a)
                );




        Map<List<String>, Object> withPrefix = splitKeys.entrySet().stream()
                .filter( x->x.getKey().size()>1 && x.getKey().get(0).equalsIgnoreCase(PREFIX) )
                .collect( Collectors.toMap(x->withoutFirst(x.getKey()), Map.Entry::getValue));

        Map<String, String> dbVerData = splitKeys.entrySet().stream().filter( x->x.getKey().size()==2 && x.getKey().get(0).equalsIgnoreCase(DB_VERSION))
                .collect( Collectors.toMap(x->withoutFirst(x.getKey()).get(0), Map.Entry::getValue));

        this.dbVersionMajor = Integer.parseInt( dbVerData.getOrDefault(MAJOR,"0"));
        this.dbVersionMinor = Integer.parseInt( dbVerData.getOrDefault(MINOR,"0"));
        this.checkDbVersion = Boolean.parseBoolean( dbVerData.getOrDefault(CHECK_DBVERSION, "false"));

        Map<String, Map<String, String>> propsByDb = withPrefix.entrySet().stream()
                .filter( x->x.getKey().size()>1 )
                .collect( Collectors.groupingBy( x-> ((Map.Entry<List<String>, Object>) x).getKey().get(0),
                        Collectors.toMap( y-> String.join(SEPERATOR,
                                        withoutFirst(((Map.Entry<List<String>, Object>) y).getKey())),
                                z->((Map.Entry<List<String>, Object>) z).getValue().toString())
                        )
                );

        LOGGER.warn( "props "+propsByDb);
        envCredMap = new HashMap<>();
        dataSources = new HashMap<>();
        smokDataSourceMap = new HashMap<>();
        smokCassMap = new HashMap<>();
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
                LOGGER.warn(" DB main "+longKey+" :"+envValue);
                map.put(longKey, val);
            }
        }
        LOGGER.warn("Read "+map.size()+" env and property items");

        Map<String, Object> dbEnvMap = allProp.entrySet().stream().
                filter( en->en.getKey().startsWith(PREFIX+SEPERATOR)).
                collect(Collectors.toMap(en->en.getKey().substring(PREFIX.length()+1),en->en.getValue()));

        Set<String> names =  dbEnvMap.keySet().stream().map(x->x.substring(0,x.indexOf(SEPERATOR))).collect(Collectors.toSet());

        LOGGER.warn("Read "+names.size()+" DBs to connect to: "+names);

        for(String smkDS : names){
            SmokDatasourceName sdsn = new SmokDatasourceName(smkDS);
            Map<String, Object> credData =
                    dbEnvMap.entrySet().stream().filter( en->en.getKey().startsWith(sdsn.getDataSourceName()))
                            .collect(Collectors.toMap( x->x.getKey(), y-> y.getValue()));
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
//                    if (oVal instanceof Number ) {
//                        long n = ((Number) oVal).longValue();
//                        value = "" + n;
//                    } else if (oVal instanceof Boolean) {
//                        value = ((Boolean) oVal).toString();
//                    } else {
                        value = (String) credData.get(key);
//                    }
                }
                found++;
                credMap.put(field, value);
            }

            smokCassMap = new HashMap<>();

            if (found >= CredFields.length) {
                DBCreds creds = new DBCreds(credMap);
                try {
                    envCredMap.put(sdsn, creds);
                    envCredList.add(creds);
                    SmokDataSource src = new SmokDataSource(creds);
                    smokDataSourceMap.put(sdsn, src);
                    if (creds.getVariant() == DatabaseVariant.CASSANDRA) {
                        CassandraConnector cassConn = new CassandraConnector();
                        cassConn.connect(creds, cassandraState);
                        CqlSession cqlSess = cassConn.getSession();
                        if (cqlSess==null){
                          LOGGER.error("Error getting CqlSession "+sdsn+" "+cassConn.getMessage());
                          LOGGER.error("Creds "+sdsn+" ");
                          continue;
                        }
                        cqlSess.execute("USE " + CqlIdentifier.fromCql(creds.getName().getDataSourceName()));
                        smokCassMap.put(sdsn, cqlSess);
                        cassandraState.setEnabled(true);
                    } else {
                        HikariDataSource hikariDataSource = src.getHikariDataSource();
                        dataSources.put(sdsn, hikariDataSource);
                    }
                } catch (Exception e){
                    LOGGER.error("Error creating datasource "+sdsn,e);
                    LOGGER.error("Creds "+sdsn+" ",e);
                }
            } else {
                String missing = Arrays.asList(CredFields).stream().map( k-> smkDS+SEPERATOR+k)
                        .filter( k->credMap.containsKey(k)).collect(Collectors.joining(", "));
                LOGGER.error("DB Config: " + sdsn + " missing fields "+missing);
            }
        }
    }

    public DBCreds getCreds(SmokDatasourceName dbName){
        DBCreds creds =  envCredMap.get(dbName);
        if (creds ==null){
            LOGGER.warn("creds no creds for dbName "+dbName);
            creds = new DBCreds();
        }
        return creds;
    }

    public int getDbVersionMajor() {
        return dbVersionMajor;
    }

    public int getDbVersionMinor() {
        return dbVersionMinor;
    }

    public boolean isCheckDbVersion() {
        return checkDbVersion;
    }

    public HikariDataSource getHikDatasource(SmokDatasourceName dbSrcName)
    {
        return dataSources.get(dbSrcName);
    }

    public SmokDataSource getSmokDatasource(SmokDatasourceName dbSrcName){
        return smokDataSourceMap.get(dbSrcName);
    }

    public CqlSession getCqlSession(SmokDatasourceName dbSrcName){
        return smokCassMap.get(dbSrcName);
    }

    public SmokDatasourceName getMainCassDBName(){
        DBCreds creds = envCredList.stream().filter(x-> x.getVariant()== DatabaseVariant.CASSANDRA)
                .findFirst().orElse(new DBCreds())
        ;
        if (creds ==null){
            LOGGER.warn("creds no creds for dbName main");
            creds = new DBCreds();
        }
        return creds.getName();
    }

    public SmokDatasourceName getMainSqlDBName(){
        DBCreds creds = envCredList.stream().filter(x->x.getVariant() !=DatabaseVariant.CASSANDRA)
                .findFirst().orElse(null);
                ;
        if (creds ==null){
            LOGGER.warn("creds no creds for dbName main sql ");
            creds = new DBCreds();
        }
        return creds.getName();
    }

    public SmokDatasourceName getMainDBName(){
        DBCreds creds = envCredList.stream()
                .findFirst().orElse(null)
                ;
        if (creds ==null){
            LOGGER.warn("creds no creds for dbName main db");
            creds = new DBCreds();
        }
        return creds.getName();
    }


}
