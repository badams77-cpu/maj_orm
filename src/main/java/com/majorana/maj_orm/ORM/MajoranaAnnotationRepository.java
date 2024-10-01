package com.majorana.maj_orm.ORM;

import com.majorana.maj_orm.Utils.MethodPrefixingLoggerFactory;
import com.majorana.maj_orm.DBs.MajDataSourceName;
//import com.majorana.ORM.domain.entity.BaseSmokEntity;
import com.majorana.maj_orm.Utils.SQLHelper;
import com.majorana.maj_orm.persist.newannot.*;
import com.majorana.maj_orm.persist.newannot.*;
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
import java.util.Date;
import java.util.stream.Collectors;

public class MajoranaAnnotationRepository<T extends BaseMajoranaEntity> {

    private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(MajoranaAnnotationRepository.class);

    private static final String PACKAGE_BASE = "com.majorana.ORM";

    private static final LocalDate defDate = LocalDate.of(1970,1,1);

    private static final LocalTime defTime = LocalTime.of(0,0,0);

    private static final LocalTime oneSecPast = LocalTime.of(0,0,1);

    private static final LocalDateTime defDateTime = defDate.atTime(oneSecPast);

    private static final String[] columnClassNames = { jakarta.persistence.Column.class.getCanonicalName() };

    protected Class<T> clazz;

    protected MajoranaDBConnectionFactory dbFactory;

    protected MajDataSourceName dbName;

    protected List<MajoranaRepositoryField> repoFields = new LinkedList<>();

    protected Method preSave;

    protected Method postLoad;


    public MajoranaAnnotationRepository(MajoranaDBConnectionFactory dbFactory,  MajDataSourceName dbName ,Class<T> clazz){
        this.dbFactory = dbFactory;
        this.dbName = dbName;
        this.clazz =clazz;
        this.repoFields = setFieldsByReflection(clazz);
        findMethods(clazz);
    }

    public static int mapParams(java.sql.PreparedStatement ps, Object[] args) throws SQLException {
        int i = 1;
        for (Object arg : args) {
            if (arg instanceof Date) {
                ps.setTimestamp(i++, new Timestamp(((Date) arg).getTime()));
            } else if (arg instanceof Integer) {
                ps.setInt(i++, (Integer) arg);
            } else if (arg instanceof Long) {
                ps.setLong(i++, (Long) arg);
            } else if (arg instanceof Double) {
                ps.setDouble(i++, (Double) arg);
            } else if (arg instanceof Float) {
                ps.setFloat(i++, (Float) arg);
            } else if (arg instanceof java.util.Date){
                ps.setTimestamp(i++, new java.sql.Timestamp( ((java.util.Date) arg).getTime()));
            } else {
                ps.setString(i++, (String) arg);
            }
        }
        return i;
    }

    public static List<MajoranaRepositoryField> getRepositoryFields(Class c){
       return setFieldsByReflection(c);
    }

    public String getReadStringNP(String table, String[] paramNames , Object[] params){
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



    public String getReadStringNPSelectClause(String table, String sql1, String[] paramNames , Object[] params){
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

        if (sql1!=null){
            buffy.append( sql1);
        }
        buffy.append(";");
        return buffy.toString();
    }

    public String getReadStringSelectClause(String table, String sql1){
        StringBuffer buffy =  new StringBuffer();
//        SqlParameterSource params = getSqlParameterSource(sUser);
        buffy.append("SELECT "+ repoFields.stream().filter(x->!x.isTransient()).map(x->x.getDbColumn()).collect(Collectors.joining(",") ));
        buffy.append(" FROM "+table );

        if (sql1!=null){
            buffy.append( sql1);
        }
        buffy.append(";");
        return buffy.toString();
    }


    public String getReadString(String table){
        StringBuffer buffy =  new StringBuffer();
//        SqlParameterSource params = getSqlParameterSource(sUser);
        buffy.append("SELECT "+ repoFields.stream().filter(x->!x.isTransient()).map(x->x.getDbColumn()).collect(Collectors.joining(",") ));
        buffy.append(" FROM "+table );

        buffy.append(";");
        return buffy.toString();
    }

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

    public String getCreateStringNP(T sUser){
        StringBuffer buffy =  new StringBuffer();
        SqlParameterSource params = getSqlParameterSource(sUser);
        buffy.append("("+ repoFields.stream().filter(x->!x.isTransient()).map(x->x.getDbColumn()).collect(Collectors.joining(",") )+ ")");
        buffy.append(" VALUES ("+ repoFields.stream().filter(x->!x.isTransient())
                .map(x->x.isPopulatedCreated() || x.isPopulatedUpdated()? "now()": ":"+x.getDbColumn())
                .collect(Collectors.joining(",") )+ ");");
        return buffy.toString();
    }

    public List<MajoranaRepositoryField> getRepoFields() {
        return repoFields;
    }

    public String getUpdateStringNP(T sUser){
        StringBuffer buffy =  new StringBuffer();
        buffy.append(" SET "+ repoFields.stream().filter(x->!x.isTransient()).filter(x->x.isUpdateable())
                .map(x->x.getDbColumn() + " = :" + getIdField().getDbColumn())
                .collect(Collectors.joining(",") )+ " WHERE "+getIdField().getDbColumn()+"=:"+getIdField().getDbColumn());
        return buffy.toString();
    }

    public String getUpAltIdStringNP(T sUser){
        StringBuffer buffy =  new StringBuffer();
        buffy.append(" SET "+ repoFields.stream().filter(x->!x.isTransient()).filter(x->x.isAltId())
                .map(x->x.getDbColumn() + " = " + ((x.isPopulatedUpdated())?"now() " : ":"+x.getDbColumn()))
                .collect(Collectors.joining(",") ));
        return buffy.toString();
    }

    public String getUpdateStringNPSansWhere(T sUser){
        StringBuffer buffy =  new StringBuffer();
        buffy.append(" SET "+ repoFields.stream().filter(x->!x.isTransient()).filter(x->x.isUpdateable())
                .map(x->x.getDbColumn() + " = " + ((x.isPopulatedUpdated())?"now() " : ":"+x.getDbColumn()))
                .collect(Collectors.joining(",") ));
        return buffy.toString();
    }

    public String getSqlFieldString(){
        StringBuffer buffy =  new StringBuffer();
        buffy.append(repoFields.stream().filter(x->!x.isTransient()).map(x->x.getDbColumn()).collect(Collectors.joining(", ") ) );
        return buffy.toString();
    }


    public String getSqlFieldStringWithPrefix(String s){
        StringBuffer buffy =  new StringBuffer();
        buffy.append(repoFields.stream().filter(x->!x.isTransient()).map(x->s+","+x.getDbColumn()).collect(Collectors.joining(", ") ) );
        return buffy.toString();
    }


    protected String getCreateString(T sUser){
        StringBuffer buffy =  new StringBuffer();
        SqlParameterSource params = getSqlParameterSource(sUser);
        buffy.append("("+ repoFields.stream().filter(x->!x.isTransient()).map(x->x.getDbColumn()).collect(Collectors.joining(",") )+ ")");
        buffy.append(" VALUES ("+ repoFields.stream().filter(x->!x.isTransient())
                .map(x->x.isPopulatedCreated() || x.isPopulatedUpdated()? "now()": "?")
                .collect(Collectors.joining(",") )+ ");");
        return buffy.toString();
    }

    protected String getUpdateString(T sUser){
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

    public MajoranaRepositoryField getIdField(){
        return repoFields.stream().filter( rf->rf.isId()   && (rf.getValueType()==int.class  || rf.getValueType()==long.class()).findFirst().orElse(null);
    }

    public static boolean isInStringArray( String potentialTargets[], String test){
        return Arrays.stream(potentialTargets).anyMatch( pt -> pt.equals(test));
    }

    public String getKeyUuid(){
        return repoFields.stream().filter(
                rf -> rf.isId() && rf.getValueType()==java.util.UUID.class
        ).map( rf->rf.getDbColumn())

                .findFirst().orElse("");
    }

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
        LOGGER.trace(" Searching annotations for field "+name+" on class "+clazz.getCanonicalName());
        ret.addAll(Arrays.stream(annotations).collect(Collectors.toUnmodifiableList()));
        while(clazz!=null && clazz!=Object.class){
            clazz = clazz.getSuperclass();
            if (clazz==null || clazz==Object.class){ break; }
            try {
                Field newField = clazz.getDeclaredField(name);
                //Field newField = fields.stream().filter(f -> f.getName().equals(name)).findFirst().orElse(null);
                if (newField != null) {
                    Annotation[] annot = newField.getDeclaredAnnotations();
                    ret.addAll(Arrays.stream(annotations).collect(Collectors.toUnmodifiableList()));
                 //   continue;
                }
            } catch (NoSuchFieldException e){
                LOGGER.trace("Class "+ clazz+" no field "+name);
            }

        }
        LOGGER.trace("CLass "+clazz+ " found "+ ret.size()+" new field annotations: ("+ret.stream()
                .map(a->a.annotationType().getCanonicalName()).collect(Collectors.joining(", ")));
        return ret.toArray(new Annotation[0]);
    }

    private static List<MajoranaRepositoryField> setFieldsByReflection(Class clazz){



        List<MajoranaRepositoryField> repoFields = new LinkedList<>();
        Set<String> setNames = new HashSet<>();
        List<Field> fields = new LinkedList<> ();
        List<Method> methods = new LinkedList<>();
        while(clazz!=null && clazz!=Object.class) {
            fields.addAll(getClassFields(clazz));
            methods.addAll(getClassMethods(clazz));
            for (Field field : fields) {
                if (clazz.getName().equals("JunkUser")) {
                    field = field;

                }
                Annotation[] annotations = getAnnotationsOnClassAndSuper(field, clazz); //field.getDeclaredAnnotations();
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
                if (isStatic) {
                    continue;
                }
                boolean isTransient = Modifier.isTransient(field.getModifiers());
                majoranaField.setTransient(isTransient);

                boolean updateable = false;
                boolean popCreated = false;
                boolean popUpdated = false;
                boolean nullable = false;
                boolean isAltId = false;
                boolean isId = false;
                boolean hasId = false;

                for (Annotation ann : annotations) {
                    if (ann.annotationType().equals(jakarta.persistence.Id.class) && !hasId) {
                        isId = true;
                        hasId = true;
                    }
                    if (ann.annotationType().equals(Updateable.class)) {
                        updateable = true;
                    }
                    if (ann.annotationType().equals(AltID.class)) {
                        isAltId = true;
                    }
                    if (ann.annotationType().equals(PopulatedCreated.class)) {
                        popCreated = true;
                    }
                    if (ann.annotationType().equals(PopulatedUpdated.class)) {
                        popUpdated = true;
                    }
                    if (ann.annotationType().equals(Nullable.class)) {
                        nullable = true;
                    }

                    if (isInStringArray(columnClassNames, ann.annotationType().getCanonicalName())) {
                        toAdd = true;
//                    Column column = field.getAnnotation(Column.class);
                        Column column = (Column) ann;
                        String dbField = column.name();
                        if (dbField != null) {
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
                majoranaField.setAltId(isAltId);
                //   Class curMethodClass = clazz;
                //   while(clazz!=null && clazz!=Object.class){
                //        methods = getClassMethods(curMethodClass);
                for (Method method : methods) {
                    if (isGetter(method) && method.getName().equalsIgnoreCase("GET" + field.getName())
                            || method.getName().equalsIgnoreCase("IS" + field.getName().toUpperCase())
                            || (method.getName().equalsIgnoreCase(field.getName().toUpperCase()))
                    ) {
                        if (majoranaField.getGetter() == null) {
                            majoranaField.setGetter(method);
                        }
                    } else if (isSetter(method) && method.getName().equalsIgnoreCase("SET" + field.getName())) {
                        if (majoranaField.getSetter() == null) {
                            majoranaField.setSetter(method);
                        }
                    }
                }
                //       curMethodClass = curMethodClass.getSuperclass();
                //   }

                boolean haveRequiredData = majoranaField.checkFields();
                if (toAdd && haveRequiredData && !setNames.contains(majoranaField.getName())) {
                    repoFields.add(majoranaField);
                    setNames.add(majoranaField.getName());
                } else if (!haveRequiredData) {
                    LOGGER.warn("setFieldsByReflection: Class: " + clazz.getName() + " Field: " + majoranaField.getName() + " missing info " + majoranaField.getMissing());
                }
            }
            clazz = clazz.getSuperclass();
        }
        if (!repoFields.stream().anyMatch(f->f.isId()) && repoFields.stream().anyMatch(f->f.isAltId())){
            MajoranaRepositoryField alt = repoFields.stream().filter(f->f.isAltId()).findFirst().orElse(null);
            alt.setAltId(false);
            alt.setId(true);
        }
        return repoFields;
    }

    protected PreparedStatementCreator getSqlPreparedStatementParameter(String sql, T entity, boolean genKey) {

     //   Connection conn = dbFactory.getMysqlConn(dbName)s
        PreparedStatementCreator pc = new MajorPreparedStatCreator() {


            protected boolean genKey;

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

    protected SqlParameterSource getSqlParameterSource(T entity){
        return new MapSqlParameterSource(getParameterMap(entity));
    }

    public SqlParameterSource getSqlParameterSourceWithDeletedAt(MajDataSourceName sDn,T entity){
        Map<String, Object> sourceMap =getParameterMap(entity);
        sourceMap.put("deleted_at", (Boolean) sourceMap.getOrDefault("deleted",false)?  dbFactory.getDBTime(sDn) : SQLHelper.BLANK_TIMESTAMP);
        return new MapSqlParameterSource(sourceMap);
    }

    public Map<String, Object> getParameterMapWithDeletedAt(MajDataSourceName sDn,T entity){
        Map<String, Object> sourceMap =getParameterMap(entity);
        sourceMap.put("deleted_at", (Boolean) sourceMap.getOrDefault("deleted",false)?  dbFactory.getDBTime(sDn) : SQLHelper.BLANK_TIMESTAMP);
        return sourceMap;
    }

    protected Timestamp getDeletedA(BaseMajoranaEntity bse){
        return Timestamp.valueOf (bse.isDeleted() ? bse.getDeletedAt() : SQLHelper.BLANK_TIMESTAMP);
    }

    public Map<String, Object> getParameterMap(T entity){
        if (preSave!=null){
            invokeMethod(entity, preSave);
        }
        Map<String, Object> sourceMap = new HashMap<>();
        for(MajoranaRepositoryField field : repoFields){
            try {
                Object ob = invokeGetter(entity, field.getGetter());
                if (ob!=null) {
                    if (ob instanceof UUID){
                        ob = ob.toString();
                    } else

                    if (ob instanceof java.sql.Blob) {
                        // ok
                    } else if (ob instanceof LocalDate) {
                        ob = java.sql.Date.valueOf((LocalDate) ob);
                    } else if (ob instanceof LocalTime) {
                        ob = Time.valueOf((LocalTime) ob);
                    } else if (ob instanceof LocalDateTime) {
                        ob = Timestamp.valueOf((LocalDateTime) ob);
                    } else if (field.getValueType().isEnum()) {
                        ob = ((Enum) ob).name();
                    } else if (ob instanceof java.util.Date) {
                        ob = new java.sql.Timestamp(((Date) ob).getTime());
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

    public static void setRandom(List<MajoranaRepositoryField> lmf, Object ob, Random r){
        int cnt=0;
        for(MajoranaRepositoryField field : lmf){
            try {
               if (field.getValueType()==Blob.class){
                   byte[] by = new byte[1];
                   by[0]= (byte) r.nextInt(127);
                   Blob b = new javax.sql.rowset.serial.SerialBlob(by);

               }
            if (field.getValueType()==UUID.class) {
                UUID ns =  UUID.randomUUID();
                invokeSetter( ob, ns, field.getSetter());
            } else if (field.getValueType()==LocalDate.class) {
                int y = 1970 + r.nextInt(100);
                int m = r.nextInt(12)+1;
                int d = daysInMonth( y, m);
                LocalDate ld = LocalDate.of( y, m, d);
                invokeSetter( ob, ld, field.getSetter());
            } else if (field.getValueType()== LocalTime.class) {
                LocalTime lt = LocalTime.of( r.nextInt(24), r.nextInt(60), r.nextInt(60));
                invokeSetter( ob, lt, field.getSetter());
            } else if (field.getValueType()== LocalDateTime.class) {
                int y = 1970 + r.nextInt(100);
                int m = r.nextInt(12) + 1;
                int d = daysInMonth(y, m);
                LocalTime lt = LocalTime.of(r.nextInt(24), r.nextInt(60), r.nextInt(60));
                LocalDate ld = LocalDate.of(y, m, d);
                LocalDateTime ldt = ld.atTime(lt);
                invokeSetter(ob, ldt, field.getSetter());
            } else if (field.getValueType().equals(java.util.Date.class)){
                Date d = new Date(r.nextLong(1000000000L));
                invokeSetter(ob, d, field.getSetter());
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
                    case "java.util.Date":
                        invokeSetter( ob, new java.util.Date(r.nextLong(100000000000L)),  field.getSetter());
                        break;
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
                    if (field.getValueType()==Blob.class){
                        ps.setBlob(i, (java.sql.Blob) ob);
                    }
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
                                case "java.util.Date":
                                    java.sql.Timestamp dt =new java.sql.Timestamp(((java.util.Date)ob).getTime());
                                    ps.setTimestamp(i, dt);
                                    break;
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


    public RowMapper<T> getMapper(){
        return new RepositoryFieldMapper();
    }

    public RowMapper<Integer> getIntegerMapper(){
        return new RowMapper<Integer>() {
            @Override
            public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
                return rs.getInt(1);
            }
        };
    }

    protected class RepositoryFieldMapper implements RowMapper<T> {

        private static final Logger LOGGER = MethodPrefixingLoggerFactory.getLogger(MajoranaAnnotationRepository.RepositoryFieldMapper.class);

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
                        if (java.sql.Blob.class.isAssignableFrom(field.getValueType())){
                            invokeSetter(entity, rs.getBlob(col), setter);
                        } else
                        if (field.getValueType().isEnum()) {
                            try {
                                Object value = null;
                                if (rs.getString(col)==null){
                                    Method valueOf = field.getValueType().getMethod("valueOf", String.class);
                                    value = valueOf.invoke(null, "UNKNOWN");
                                } else {
                                    Method valueOf = field.getValueType().getMethod("valueOf", String.class);
                                    value = valueOf.invoke(null, rs.getString(col));
                                }
                                invokeSetter(entity, value, setter);
                            } catch (Exception e) {
                                Method valueOf = field.getValueType().getMethod("fromString", String.class);
                                Object value = valueOf.invoke(null, rs.getString(col));

                                if (value!=null){ invokeSetter(entity, value, setter); }
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
                                    String s = rs.getString(col);
                                    invokeSetter(entity, s==null ? null : UUID.fromString(s), setter);
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
                                case "java.util.Date":
                                    java.sql.Timestamp ti = rs.getTimestamp(col);
                                    invokeSetter(entity, new java.util.Date(ti.getTime()), setter);
                                    break;
                                case "java.time.LocalDate":
                                    java.sql.Date date1 = rs.getDate(col);
                                    LocalDate ld = date1 != null ? date1.toLocalDate() : null;
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
    public static void invokeSetter(Object obj,Object variableValue,Method setter) throws IllegalAccessException, IllegalArgumentException , InvocationTargetException
    {
            setter.invoke(obj,variableValue);
    }

    public static Object invokeGetter(Object obj,Method getter) throws IllegalAccessException, IllegalArgumentException , InvocationTargetException
    {
            if (obj==null){
                return null;
            }
            Object f = getter.invoke(obj);
            return f;
    }

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
        while (supClass!=null && supClass!=Object.class){
            Method[] supMethods = supClass.getDeclaredMethods();
            for(Method method : supMethods){
                out.add(method);
            }
            supClass = supClass.getSuperclass();
        }
        return out;
    }

}
