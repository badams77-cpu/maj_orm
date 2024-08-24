package com.majorana.ORM;

import com.majorana.Utils.MethodPrefixingLoggerFactory;
import com.majorana.DBs.MajDatasourceName;
import com.majorana.persist.newannot.*;
//import com.majorana.ORM.domain.entity.BaseMajEntity;
import com.majorana.Utils.SQLHelper;
import jakarta.persistence.Column;
import org.slf4j.Logger;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;


import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

/**
 *  THis is the keys ORM class in the Maj System, it reads an CLass type by reflect, and its
 *  jakarta and Majorana ORM annotations to persist to the database and back
 *
 * @param <T>
 */



public class MajoranaAnnotationRepository<T extends BaseMajoranaEntity> {

    private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(MajoranaAnnotationRepository.class);

    private static final String PACKAGE_BASE = "Majorana.ORM";

    private static final LocalDate defDate = LocalDate.of(1970,1,1);

    private static final LocalTime defTime = LocalTime.of(0,0,0);

    private static final LocalTime oneSecPast = LocalTime.of(0,0,1);

    private static final LocalDateTime defDateTime = defDate.atTime(oneSecPast);

    private static final String[] columnClassNames = { jakarta.persistence.Column.class.getCanonicalName() };

    public Class<T> clazz;

    public MajoranaDBConnectionFactory dbFactory;

    public MajDatasourceName dbName;

    public List<MajoranaRepositoryField> repoFields = new LinkedList<>();

    public Method preSave;

    public Method postLoad;

    /**
     * Creates a Repository class for a type of class clazz, using DB name dbName
     * and using the dbFactory which the Majorana ORM sets up with the environment data for
     * its credentials
     *
     * @param dbFactory - A majorana db Factory
     * @param dbName String
     * @param clazz Class
     */


    public MajoranaAnnotationRepository(MajoranaDBConnectionFactory dbFactory,  MajDatasourceName dbName ,Class<T> clazz){
        this.dbFactory = dbFactory;
        this.dbName = dbName;
        this.clazz =clazz;
        this.repoFields = setFieldsByReflection(clazz);
        findMethods(clazz);
    }

    /**
     * produces a list of fields to persist from the data on the class c
     *
     * @param c
     * @return
     */

    public static List<MajoranaRepositoryField> getRepositoryFields(Class c){
       return setFieldsByReflection(c);
    }

    /**
     *  String getReadString(...)
     *
     *  Given a table and parameter names and values, produces a SQL string to find the data in
     *  the given database table
     *
     * @param table
     * @param paramNames
     * @param params
     * @return
     */

    public String getReadString(String table, String[] paramNames , Object[] params){
        StringBuffer buffy =  new StringBuffer();
//        SqlParameterSource params = getSqlParameterSource(sUser);
        buffy.append("SELECT "+ repoFields.stream().filter(x->!x.isTransient()).map(x->x.getDbColumn()).collect(Collectors.joining(",") ));
        buffy.append(" FROM "+table );
        List<String>  paramsList = new LinkedList<>();
        for(int i=0; i<Math.min(paramNames.length, params.length); i++){
            if (params[i]!=null && paramNames[i]!=null) {
                String t = paramNames[i] + " =? ";
                paramsList.add(t);
            }
        }

        if (paramsList!=null){
            buffy.append( " WHERE "+  paramsList.stream().collect(Collectors.joining(", ")));
        }
        buffy.append(";");
        return buffy.toString();
    }

    /**
     *  String delete String
     *
     *  Given a table and params to match, form a delete SQL string to perform
     *  a database deletion.
     *
     * @param table
     * @param paramNames
     * @param params
     * @return sql string
     */

    public String getDeleteString(String table, String[] paramNames , Object[] params){
        StringBuffer buffy =  new StringBuffer();
//        SqlParameterSource params = getSqlParameterSource(sUser);
        buffy.append("DELETE ");
        //+ repoFields.stream().filter(x->!x.isTransient()).map(x->x.getDbColumn()).collect(Collectors.joining(",") ));
        buffy.append(" FROM "+table );
        List<String>  paramsList = new LinkedList<>();
        for(int i=0; i<Math.min(paramNames.length, params.length); i++){
            if (params[i]!=null && paramNames[i]!=null) {
                String t = paramNames[i] + " =? ";
                paramsList.add(t);
            }
        }

        if (paramsList!=null){
            buffy.append( " WHERE "+  paramsList.stream().collect(Collectors.joining(", ")));
        }
        buffy.append(";");
        return buffy.toString();
    }

    /**
     * Create a INsert string to store an entity  of class T in the database, uses named paramters
     *
     * @param sUser
     * @return sql String
     */

    public String getCreateStringNP(T sUser){
        StringBuffer buffy =  new StringBuffer();
        SqlParameterSource params = getSqlParameterSource(sUser);
        buffy.append("("+ repoFields.stream().filter(x->!x.isTransient()).map(x->x.getDbColumn()).collect(Collectors.joining(",") )+ ")");
        buffy.append(" VALUES ("+ repoFields.stream().filter(x->!x.isTransient())
                .map(x->x.isPopulatedCreated() || x.isPopulatedUpdated()? "now()": ":"+x.getField().getName())
                .collect(Collectors.joining(",") )+ ");");
        return buffy.toString();
    }

    /**
     * Creates an update string o update a value in the database from its entity type using
     * named parameters
     *
     * @param sUser
     * @return
     */

    public String getUpdateStringNP(T sUser){
        StringBuffer buffy =  new StringBuffer();
        buffy.append(" SET "+ repoFields.stream().filter(x->!x.isTransient()).filter(x->x.isUpdateable())
                .map(x->x.getDbColumn() + ":" + ((x.isPopulatedUpdated())?"now() " : ":"+x.getField().getName()))
                .collect(Collectors.joining(",") )+ " WHERE id=:id");
        return buffy.toString();
    }

    /**
     * CReates an SQL select clause listing the all the db columns to retrieve from the database
     *
     * @return SQL fields for the select clause
     */

    public String getSqlFieldString(){
        StringBuffer buffy =  new StringBuffer();
        buffy.append(repoFields.stream().filter(x->!x.isTransient()).map(x->x.getDbColumn()).collect(Collectors.joining(", ") ) );
        return buffy.toString();
    }


    public String getCreateString(T sUser){
        StringBuffer buffy =  new StringBuffer();
        SqlParameterSource params = getSqlParameterSource(sUser);
        buffy.append("("+ repoFields.stream().filter(x->!x.isTransient()).map(x->x.getDbColumn()).collect(Collectors.joining(",") )+ ")");
        buffy.append(" VALUES ("+ repoFields.stream().filter(x->!x.isTransient())
                .map(x->x.isPopulatedCreated() || x.isPopulatedUpdated()? "now()": "?")
                .collect(Collectors.joining(",") )+ ");");
        return buffy.toString();
    }

    public String getUpdateString(T sUser){
        StringBuffer buffy =  new StringBuffer();
        buffy.append(" SET "+ repoFields.stream().filter(x->!x.isTransient()).filter(x->x.isUpdateable())
                .map(x->x.getDbColumn() + ":" + ((x.isPopulatedUpdated())?"now() " : "?"))
                .collect(Collectors.joining(",") )+ " WHERE id=:id");
        return buffy.toString();
    }

    private void findMethods(Class clazz){
        List<Method> methods = getClassMethods(clazz);
        for(Method meth : methods){
            Annotation[] annotations = meth.getDeclaredAnnotations();
            for(Annotation ann : annotations){
                if (ann.annotationType().equals(Postload.class)){
                    postLoad = meth;
                }
                if (ann.annotationType().equals(Presave.class)){
                    preSave = meth;
                }
            }
        }
    }

    /**
     * Tests if a target string is in a list
     *
     *
     * @param potentialTargets
     * @param test
     * @return
     */

    public static boolean isInStringArray( String potentialTargets[], String test){
        return Arrays.stream(potentialTargets).anyMatch( pt -> pt.equals(test));
    }

    /**
     * Creates a random key UUID
     *
     * @return String of the UUID
     */

    public String getKeyUuid(){
        return repoFields.stream().filter(
                rf -> rf.isId() && rf.getValueType()==java.util.UUID.class
        ).map( rf->rf.getDbColumn())

                .findFirst().orElse("");
    }

    /**
     *  Creates a random key ID
     *
     * @return random key stream
     */

    public String getKeyId(){
        return repoFields.stream().filter(
                        rf -> rf.isId() && rf.getValueType()==Integer.class
                ).map( rf->rf.getDbColumn())

                .findFirst().orElse("");
    }

    private static Annotation[] getAnnotationsOnClassAndSuper( Field field, Class clazz){
        List<Annotation> ret = new LinkedList<>();
        Annotation[] annotations = field.getDeclaredAnnotations();
        String name = field.getName();
        LOGGER.warn(" Searching annotations for field "+name+" on class "+clazz.getCanonicalName());
        ret.addAll(Arrays.stream(annotations).collect(Collectors.toUnmodifiableList()));
        while(ret.isEmpty()){
            clazz = clazz.getSuperclass();
            if (clazz==null){ break; }
            try {
                Field newField = clazz.getDeclaredField(name);
                //Field newField = fields.stream().filter(f -> f.getName().equals(name)).findFirst().orElse(null);
                if (newField != null) {
                    Annotation[] annot = newField.getDeclaredAnnotations();
                    ret.addAll(Arrays.stream(annotations).collect(Collectors.toUnmodifiableList()));
                    break;
                }
            } catch (NoSuchFieldException e){
                LOGGER.warn("Class "+ clazz+" no field "+name);
            }

        }
        LOGGER.info("CLass "+clazz+ " found "+ ret.size()+" new field annotations: ("+ret.stream()
                .map(a->a.annotationType().getCanonicalName()).collect(Collectors.joining(", ")));
        return ret.toArray(new Annotation[0]);
    }

    private static List<MajoranaRepositoryField> setFieldsByReflection(Class clazz){

        List<MajoranaRepositoryField> repoFields = new LinkedList<>();

        List<Field> fields = getClassFields(clazz);
        List<Method> methods = getClassMethods(clazz);
        for(Field field : fields){
            Annotation[] annotations =  getAnnotationsOnClassAndSuper(field, clazz); //field.getDeclaredAnnotations();
            boolean toAdd = false;
            MajoranaRepositoryField majoranaField = new MajoranaRepositoryField();
            majoranaField.setField(field);
            majoranaField.setValueType(field.getType());
            majoranaField.setName(field.getName());
            majoranaField.setDbColumn(field.getName());

//            if (field.getName().equals("createdByUserEmail")){
//                LOGGER.warn("createdByUserEmail");
//            }


            boolean isStatic = Modifier.isStatic(field.getModifiers());
            if  (isStatic){ continue; }
            boolean isTransient = Modifier.isTransient(field.getModifiers());
            majoranaField.setTransient(isTransient);

            boolean updateable = false;
            boolean popCreated = false;
            boolean popUpdated = false;
            boolean nullable = false;
            boolean isId = false;
            boolean hasId = false;
            for(Annotation ann : annotations){
                if (ann.annotationType().equals(jakarta.persistence.Id.class) && !hasId) {
                    isId = true;
                    hasId=true;
                }
                if (ann.annotationType().equals(Updateable.class)){
                    updateable = true;
                }
                if (ann.annotationType().equals(PopulatedCreated.class)){
                    popCreated = true;
                }
                if (ann.annotationType().equals(PopulatedUpdated.class)){
                    popUpdated = true;
                }
                if (ann.annotationType().equals(Nullable.class)){
                    nullable = true;
                }

                if (  isInStringArray( columnClassNames, ann.annotationType().getCanonicalName())){
                    toAdd=true;
//                    Column column = field.getAnnotation(Column.class);
                    Column column = (Column) ann;
                    String dbField = column.name();
                    if (dbField!=null){
                        majoranaField.setDbColumn(dbField);
                    }
                    majoranaField.setColumnAnnotation(column);
                }
            }
            majoranaField.setUpdateable(updateable);
            majoranaField.setPopulatedCreated(popCreated);
            majoranaField.setPopulatedUpdated(popUpdated);
            majoranaField.setNullable(nullable);
            majoranaField.setId(isId);
            for(Method method: methods){
                if (isGetter(method) && method.getName().equalsIgnoreCase("GET"+field.getName())
                        || method.getName().equalsIgnoreCase("IS"+field.getName().toUpperCase())
                        || ( method.getName().equalsIgnoreCase(field.getName().toUpperCase()) )
                ){
                    majoranaField.setGetter(method);
                } else if (isSetter(method) && method.getName().equalsIgnoreCase("SET"+field.getName())) {
                    majoranaField.setSetter(method);
                }
            }
            boolean haveRequiredData = majoranaField.checkFields();
            if (toAdd && haveRequiredData) {
                repoFields.add(majoranaField);
            } else if (!haveRequiredData){
                LOGGER.warn("setFieldsByReflection: Class: "+clazz.getName()+" Field: "+majoranaField.getName()+" missing info "+majoranaField.getMissing());
            }
        }
        return repoFields;
    }

    public PreparedStatementCreator getSqlPreparedStatementParameter(String sql, T entity, boolean genKey) {



     //   Connection conn = dbFactory.getMysqlConn(dbName)s
        PreparedStatementCreator pc = new MajorPreparedStatCreator() {


            public boolean genKey;

            public void MajorPreparedStatCreator(boolean genKey){
                this.genKey = genKey;
            }

            @Override
            public void setGenKey(boolean bol0) {

            }

            @Override
            public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
                PreparedStatement ps = genKey ? con.prepareStatement(sql,
                        Statement.RETURN_GENERATED_KEYS ) : con.prepareStatement(sql);
                if (genKey){

                }
                setPreparedStatementFields(ps, entity);
                return ps;
            }
        };

        return pc;
    }

    public SqlParameterSource getSqlParameterSource(T entity){
        return new MapSqlParameterSource(getParameterMap(entity));
    }

    /**
     * FOr a named datasource sDn and an Entity, create an SqlParameyer source to
     * store in the database, using the annotations on the entity class
     *
     * @param sDn
     * @param entity
     * @return sqlParameter sourcde
     */

    public SqlParameterSource getSqlParameterSourceWithDeletedAt(MajDatasourceName sDn,T entity){
        Map<String, Object> sourceMap =getParameterMap(entity);
        sourceMap.put("deleted_at", (Boolean) sourceMap.getOrDefault("deleted",false)?  dbFactory.getDBTime(sDn) : SQLHelper.BLANK_TIMESTAMP);
        return new MapSqlParameterSource(sourceMap);
    }

    /**
     * FOr a named database source snd am entity, creates an map comtaining the parameters
     * and names to store in the database
     *
     * @param sDn
     * @param entity
     * @return
     */

    public Map<String, Object> getParameterMapWithDeletedAt(MajDatasourceName sDn,T entity){
        Map<String, Object> sourceMap =getParameterMap(entity);
        sourceMap.put("deleted_at", (Boolean) sourceMap.getOrDefault("deleted",false)?  dbFactory.getDBTime(sDn) : SQLHelper.BLANK_TIMESTAMP);
        return sourceMap;
    }

    public Timestamp getDeletedA(BaseMajoranaEntity bse){
        return Timestamp.valueOf (bse.isDeleted() ? bse.getDeletedAt() : SQLHelper.BLANK_TIMESTAMP);
    }

    private Map<String, Object> getParameterMap(T entity){
        if (preSave!=null){
            invokeMethod(entity, preSave);
        }
        Map<String, Object> sourceMap = new HashMap<>();
        for(MajoranaRepositoryField field : repoFields){
            try {
                Object ob = invokeGetter(entity, field.getGetter());
                if (ob!=null) {
                    if (ob instanceof LocalDate) {
                        ob = java.sql.Date.valueOf((LocalDate) ob);
                    } else if (ob instanceof LocalTime) {
                        ob = Time.valueOf((LocalTime) ob);
                    } else if (ob instanceof LocalDateTime) {
                        ob = Timestamp.valueOf((LocalDateTime) ob);
                    } else if (field.getValueType().isEnum()) {
                        ob = ((Enum) ob).name();
                    } else {
                        // Default
                    }
                }
                sourceMap.put(field.getDbColumn(), ob);
            } catch (Exception e){
                Exception f=e;
                LOGGER.warn("getSqlParametersource: Error Serializing field "+field.getName()+" "+field.getDbColumn(),f);

            }
        }
        return sourceMap;
    }

    private static int daysInMonth( int year, int month){
        LocalDate date1 = LocalDate.of(year, month, 1);
        LocalDate date2 = date1.plusMonths(1);
        long day1 = ChronoUnit.DAYS.between(date1, date2);
        return (int) day1;
    }

    /**
     * GIven an list of Repository fields an a empty entity, and a random number generator
     * populate the object with a random value
     *
     * @param lmf
     * @param ob
     * @param r
     */

    public static void setRandom(List<MajoranaRepositoryField> lmf, Object ob, Random r){
        int cnt=0;
        for(MajoranaRepositoryField field : lmf){
            try {
            if (field.getValueType()==UUID.class) {
                UUID ns =  UUID.randomUUID();
                invokeSetter( ob, ns, field.getSetter());
            } else if (field.getValueType()==LocalDate.class) {
                int y = 1970 + r.nextInt(100);
                int m = r.nextInt(12);
                int d = daysInMonth( y, m);
                LocalDate ld = LocalDate.of( y, m, d);
                invokeSetter( ob, ld, field.getSetter());
            } else if (field.getValueType()== LocalTime.class) {
                LocalTime lt = LocalTime.of( r.nextInt(24), r.nextInt(60), r.nextInt(60));
                invokeSetter( ob, lt, field.getSetter());
            } else if (field.getValueType()== LocalDateTime.class) {
                int y = 1970 + r.nextInt(100);
                int m = r.nextInt(12);
                int d = daysInMonth( y, m);
                LocalTime lt = LocalTime.of( r.nextInt(24), r.nextInt(60), r.nextInt(60));
                LocalDate ld = LocalDate.of( y, m, d);
                LocalDateTime ldt = ld.atTime(lt);
                invokeSetter( ob, ldt,  field.getSetter());
            } else if (field.getValueType().isEnum()) {
                List<Object> enumList =  Arrays.asList(field.getValueType().getEnumConstants());
                int e_i = r.nextInt(enumList.size());
                Enum e = (Enum) enumList.get(e_i);
                invokeSetter( ob, e,  field.getSetter());
            } else if (field.getValueType().isPrimitive()){

                switch( field.getValueType().getName()) {
                    case "int":
                        invokeSetter( ob, r.nextInt(10000),  field.getSetter());
                        break;
                    case "long":
                        invokeSetter( ob, r.nextInt(10000),  field.getSetter());
                        break;
                    case "float":
                        invokeSetter( ob, (float) (r.nextFloat(10000.0f)-5000.0f),  field.getSetter());
                        break;
                    case "double":
                        invokeSetter( ob, (double) r.nextDouble(1000000.0)-500000,  field.getSetter());
                        break;
                    case "boolean":
                        invokeSetter( ob, r.nextBoolean(),  field.getSetter());
                        break;
                }
            } else {
                switch( field.getValueType().getName()) {

                    case "java.lang.Integer":
                        invokeSetter( ob, r.nextInt(10000),  field.getSetter());
                        //  if (ob){ invokeSetter(entity, null, setter); }
                        break;
                    case "java.lang.Long":
                        invokeSetter( ob, r.nextInt(10000),  field.getSetter());
                        //invokeSetter(entity, rs.getLong(col), setter);
                        // if (rs.wasNull()){ invokeSetter(entity, null, setter); }
                        break;
                    case "java.lang.Float":
                        invokeSetter( ob, (float) (r.nextFloat(10000.0f)-5000.0f),  field.getSetter());
                        //        if (rs.wasNull()){ invokeSetter(entity, null, setter); }
                        break;
                    case "java.lang.Double":
                        invokeSetter( ob, (double) r.nextDouble(1000000.0)-500000,  field.getSetter());
//                                invokeSetter(entity, rs.getDouble(col), setter);
//                                if (rs.wasNull()){ invokeSetter(entity, null, setter); }
                        break;
                    case "java.lang.Boolean":
                        invokeSetter( ob, r.nextBoolean(),  field.getSetter());
                        //  invokeSetter(entity, rs.getBoolean(col), setter);
                        //  if (rs.wasNull()){ invokeSetter(entity, null, setter); }
                        break;
                    case "java.lang.String":
                        UUID ns =  UUID.randomUUID();
                        invokeSetter( ob, ns.toString().substring(0,12), field.getSetter());
                        //invokeSetter(entity, rs.getString(col), setter);
                        break;
                }
            }
            // Default
//                    }

//                sourceMap.put(field.getDbColumn(), ob);git
            cnt++;
        } catch (Exception e){
            Exception f=e;
            LOGGER.warn("setRandom: Error Set field "+field.getName()+" "+field.getDbColumn(),f);
        }

        }
        LOGGER.warn("setRandom generated "+cnt+" fields");
    }

    private void setPreparedStatementFields(PreparedStatement ps,T entity) throws SQLException {
        if (preSave!=null){
            invokeMethod(entity, preSave);
        }
        Map<String, Object> sourceMap = new HashMap<>();
        int i = 1;
        for(MajoranaRepositoryField field : repoFields){
            try {
                Object ob = invokeGetter(entity, field.getGetter());
                boolean isNull = ob==null;
                boolean isNullable = field.isNullable();
//                if (ob==null) {
                
//                } else {
                    if (field.getValueType()==UUID.class) {
                        String ns = isNullable ? null : UUID.randomUUID().toString();
                        ps.setString(i, isNull ? ns : ((String) ob));
                    } else if (field.getValueType()==LocalDate.class) {
                        java.sql.Date nd = isNullable? null : java.sql.Date.valueOf(defDate);
                        java.sql.Date d = isNull ? nd : java.sql.Date.valueOf((LocalDate) ob);
                        ps.setDate(i, d);
                    } else if (field.getValueType()== LocalTime.class) {
                        Time nt = isNullable? null : Time.valueOf(defTime);
                        Time t = isNull ? nt : Time.valueOf((LocalTime) ob);
                        ps.setTime(i, t);
                    } else if (field.getValueType()== LocalDateTime.class) {
                        Timestamp nts = isNullable? null : Timestamp.valueOf(defDateTime);
                        Timestamp ts = isNull ? nts : Timestamp.valueOf((LocalDateTime) ob);
                        ps.setTimestamp(i, ts);
                    } else if (field.getValueType().isEnum()) {
                        String en = isNull ? null : ((Enum) ob).name();
                        ps.setString(i, en);
                    } else if (field.getValueType().isPrimitive()){

                        switch( field.getValueType().getName()) {
                            case "int":
                                ps.setInt(i, ((Integer) ob).intValue());
                                break;
                            case "long":
                                ps.setLong(i, ((Integer) ob).longValue());
                                break;
                            case "float":
                                ps.setFloat(i, ((Float) ob).floatValue());
                                break;
                            case "double":
                                ps.setDouble(i, ((Double) ob).doubleValue());
                                break;
                            case "boolean":
                                ps.setBoolean(i, ((Boolean) ob).booleanValue());
                                break;
                         }
                    } else {
                            switch( field.getValueType().getName()) {

                                case "java.lang.Integer":
                                    ps.setInt(i, isNull ? 0 : ((Integer) ob).intValue());
                                    //  if (ob){ invokeSetter(entity, null, setter); }
                                    break;
                                case "java.lang.Long":
                                    ps.setLong(i, isNull ? 0 : ((Long) ob).longValue());
                                    //invokeSetter(entity, rs.getLong(col), setter);
                                    // if (rs.wasNull()){ invokeSetter(entity, null, setter); }
                                    break;
                                case "java.lang.Float":
                                    ps.setFloat(i, isNull ? 0 : ((Float) ob).floatValue());
                                    //        invokeSetter(entity, rs.getFloat(col), setter);
                                    //        if (rs.wasNull()){ invokeSetter(entity, null, setter); }
                                    break;
                                case "java.lang.Double":
                                    ps.setDouble(i, isNull ? 0 : ((Double) ob).doubleValue());
//                                invokeSetter(entity, rs.getDouble(col), setter);
//                                if (rs.wasNull()){ invokeSetter(entity, null, setter); }
                                    break;
                                case "java.lang.Boolean":
                                    ps.setBoolean(i, isNull ? false : ((Boolean) ob).booleanValue());
                                    //  invokeSetter(entity, rs.getBoolean(col), setter);
                                    //  if (rs.wasNull()){ invokeSetter(entity, null, setter); }
                                    break;
                                case "java.lang.String":
                                    String ns = isNullable ? null : "";
                                    ps.setString(i, isNull ? ns : ((String) ob));
                                    //invokeSetter(entity, rs.getString(col), setter);
                                    break;
                            }
                        }
                        // Default
//                    }

//                sourceMap.put(field.getDbColumn(), ob);
                i++;
            } catch (Exception e){
                Exception f=e;
                LOGGER.warn("getSqlParametersource: Error Serializing field "+field.getName()+" "+field.getDbColumn(),f);

            }
        }
        String s = ps.toString();
        LOGGER.warn("Values: "+s);
    }

    /**
     *  Gets a Jdbctemplate mapper to convert a database result to a entity T
     *
     * @return
     */

    public RowMapper<T> getMapper(){
        return new RepositoryFieldMapper();
    }

    /**
     *  Maps a single integer from an database result with one result .e.g. select count(*)
     *
     * @return
     */


    public RowMapper<Integer> getIntegerMapper(){
        return new RowMapper<Integer>() {
            @Override
            public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
                return rs.getInt(1);
            }
        };
    }

    public class RepositoryFieldMapper implements RowMapper<T> {

        @Override
        public T mapRow(ResultSet rs, int rowNum) throws SQLException {
                T entity = null;
                try {
                  entity = clazz.newInstance();
                } catch (IllegalAccessException e) {
                 LOGGER.warn("New Instance "+clazz+"IllegalAccess: ",e);
                } catch (InstantiationException e) {
                    LOGGER.warn("New Instance "+clazz+"InstantiationException ", e);
                }
                for(MajoranaRepositoryField field : repoFields) {
                    Method setter = field.getSetter();
                    String col = field.getDbColumn();
                    try {
                        if (field.getValueType().isEnum()) {
                            try {
                                Method valueOf = field.getValueType().getMethod("valueOf", String.class);
                                Object value = valueOf.invoke(null, rs.getString(col));
                                invokeSetter(entity, value, setter);
                            } catch (Exception e) {
                                Method valueOf = field.getValueType().getMethod("fromString", String.class);
                                Object value = valueOf.invoke(null, rs.getString(col));
                                invokeSetter(entity, value, setter);
                            }
                        } else {
                            switch (field.getValueType().getName()) {
                                case "int":
                                    invokeSetter(entity, rs.getInt(col), setter);
                                    break;
                                case "long":
                                    invokeSetter(entity, rs.getLong(col), setter);
                                    break;
                                case "float":
                                    invokeSetter(entity, rs.getFloat(col), setter);
                                    break;
                                case "double":
                                    invokeSetter(entity, rs.getDouble(col), setter);
                                    break;
                                case "boolean":
                                    invokeSetter(entity, rs.getBoolean(col), setter);
                                    break;
                                case "java.util.UUID":
                                    invokeSetter(entity, UUID.fromString(rs.getString(col)), setter);
                                    break;
                                case "java.lang.Integer":
                                    invokeSetter(entity, rs.getInt(col), setter);
                                    if (rs.wasNull()) {
                                        invokeSetter(entity, null, setter);
                                    }
                                    break;
                                case "java.lang.Long":
                                    invokeSetter(entity, rs.getLong(col), setter);
                                    if (rs.wasNull()) {
                                        invokeSetter(entity, null, setter);
                                    }
                                    break;
                                case "java.lang.Float":
                                    invokeSetter(entity, rs.getFloat(col), setter);
                                    if (rs.wasNull()) {
                                        invokeSetter(entity, null, setter);
                                    }
                                    break;
                                case "java.lang.Double":
                                    invokeSetter(entity, rs.getDouble(col), setter);
                                    if (rs.wasNull()) {
                                        invokeSetter(entity, null, setter);
                                    }
                                    break;
                                case "java.lang.Boolean":
                                    invokeSetter(entity, rs.getBoolean(col), setter);
                                    if (rs.wasNull()) {
                                        invokeSetter(entity, null, setter);
                                    }
                                    break;
                                case "java.lang.String":
                                    invokeSetter(entity, rs.getString(col), setter);
                                    break;
                                case "java.time.LocalDate":
                                    java.sql.Date date = rs.getDate(col);
                                    LocalDate ld = date != null ? date.toLocalDate() : null;
                                    invokeSetter(entity, ld, setter);
                                    break;
                                case "java.time.LocalTime":
                                    Time time = rs.getTime(col);
                                    LocalTime lt = time != null ? time.toLocalTime() : null;
                                    invokeSetter(entity, lt, setter);
                                    break;
                                case "java.time.LocalDateTime":
                                    Timestamp timestamp = rs.getTimestamp(col);
                                    LocalDateTime ldt = timestamp != null ? timestamp.toLocalDateTime() : null;
                                    invokeSetter(entity, ldt, setter);
                                    break;
                                default:
                                    LOGGER.warn("mapRow: Unknown column type" + field.getValueType().getName());
                                    break;
                            }

                        }

                    } catch (Exception e) {
                        LOGGER.warn("mapRow: Error deserializing field " + field.getName() + " " + field.getDbColumn(), e);
                    }
                }

                    if (postLoad != null) {
                        invokeMethod(entity, postLoad);
                    }
                    return entity;
        }

    }

    private static boolean isGetter(Method method){
        // check for getter methods
        if((method.getName().startsWith("get") || method.getName().startsWith("is"))
                && method.getParameterCount() == 0 && !method.getReturnType().equals(void.class)){
            return true;
        }
        return false;
    }

    private static boolean isSetter(Method method){
        // check for setter methods
        if(method.getName().startsWith("set") && method.getParameterCount() == 1
                && method.getReturnType().equals(void.class)){
            return true;
        }
        return false;
    }

    /**
     *  Given an Objectm, set is variable value using the java.lang.reflect setter method
     *
     *
     * @param obj
     * @param variableValue
     * @param setter
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */

    public static void invokeSetter(Object obj,Object variableValue,Method setter) throws IllegalAccessException, IllegalArgumentException , InvocationTargetException
    {
            setter.invoke(obj,variableValue);
    }

    /**
     * Given an object, read a variable using a java.lang.reflet getter method
     *
     *
     * @param obj
     * @param getter
     * @return
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */

    public static Object invokeGetter(Object obj,Method getter) throws IllegalAccessException, IllegalArgumentException , InvocationTargetException
    {
            Object f = getter.invoke(obj);
            return f;
    }

    /**
     * Given a method (java.lang.reflect), on a object, invoke a method on it
     *
     * @param obj
     * @param method
     */

        public void invokeMethod(Object obj,Method method)
        {
            try {
                method.invoke(obj, new Object[0]);
            } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e){
                LOGGER.warn("error invoking "+ clazz.getSimpleName()+":"+method.getName(), e);
            }
        }


    private static List<Field> getClassFields(Class clazz){
        Field[] fields = clazz.getDeclaredFields();
        List<Field> out = new LinkedList<Field>();
        for(Field field: fields){
            out.add(field);
        }
        Class supClass = clazz.getSuperclass();
        /**
         * TO Replace package base check with EntityVersion check
         */
        while (supClass!=null && supClass.getPackage().getName().contains(PACKAGE_BASE)){
            Field[] supFields = supClass.getDeclaredFields();
            for(Field field: supFields){
                out.add(field);
            }
            supClass = supClass.getSuperclass();
        }
        return out;
    }

    private static List<Method> getClassMethods(Class clazz){
        Method[] methods = clazz.getDeclaredMethods();
        List<Method> out = new LinkedList<Method>();
        for(Method method : methods){
            out.add(method);
        }
        Class supClass = clazz.getSuperclass();
        while (supClass.getPackage().getName().contains(PACKAGE_BASE)){
            Method[] supMethods = supClass.getDeclaredMethods();
            for(Method method : supMethods){
                out.add(method);
            }
            supClass = supClass.getSuperclass();
        }
        return out;
    }

}
