
package com.majorana.ORM_ACCESS;

import java.lang.invoke.TypeDescriptor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

import com.majorana.DBs.*;


import com.majorana.ORM.MajoranaDBConnectionFactory;
import  com.majorana.ORM.MajoranaRepositoryField;
import com.majorana.Utils.MethodPrefixingLogger;
import com.majorana.Utils.MethodPrefixingLoggerFactory;
import com.majorana.entities.BaseDistillerEntity;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;;


import com.majorana.ORM.MajoranaAnnotationRepository;
import com.datastax.oss.driver.shaded.guava.common.reflect.TypeParameter;

;
//importBoundStatement;

import static org.mockito.Mockito.mock;

// Singleton class to handle all DB access

public class DbBean implements DbBeanInterface{

  private static MethodPrefixingLogger LOGGER = MethodPrefixingLoggerFactory.getLogger(DbBean.class);

  protected static DbBeanInstance mainBean = null;

  protected static HashMap<Class, DbBeanGenericInstance> typeBeans = new HashMap<>();

    protected static DbBeanInstance getMainBean() {
        return mainBean;
    }

    public <T> List<T> getTypedList(Class T) {
        LinkedList<T> llt = new LinkedList<T>();
        return llt;
    }

    public <T extends BaseDistillerEntity> DbBeanGenericInterface<T> getTypedBean(Class<? extends BaseDistillerEntity> T) throws ClassNotFoundException, SQLException
     {
        List<T> typedList = getTypedList(T);
        this.connect();
        DbBeanGenericInstance.BeanTyper<T> typer = new DbBeanGenericInstance.BeanTyper<T>();
        return (DbBeanGenericInterface<T>) typer.getTypedBean(this, mainBean, typedList, T);
    }


    protected MajoranaDBConnectionFactory getFactory(){
        return mainBean.getFactory();
    }




    public MajoranaRepositoryField getIdField(Class T){
        MajoranaRepositoryField maj = mainBean.getRepo(T).getIdField();
        return maj;
    }

    public String getIdFieldName(Class T){
        MajoranaRepositoryField maj = mainBean.getRepo(T).getIdField();
        return maj.getDbColumn();
    }

    public DbBean(){
      mainBean = DbBeanInstance.getSingletonLazy(new CassandraState(false), new HashMap<>());
  }

    public void getSingletonLazy() {
        mainBean = DbBeanInstance.getSingletonLazy(new CassandraState(false), new HashMap<>());
    }

  protected DbBeanGenericInstance getTypedBeanFromStore(Class s){
        return typeBeans.get(s);
  }

    protected  void putTypedBeanToStore(Class s, DbBeanGenericInstance<? extends BaseDistillerEntity> b){
        typeBeans.put(s,b);
    }

  private DbBean(CassandraState state, Map<String, String> addMap){
      mainBean = DbBeanInstance.getSingletonLazy(state, addMap);
  }

    public DbBeanWithFailOverInterface getFailoverBean( List<SmokDatasourceName> names){
        List<SmokDataSource> source = names.stream().map( m ->mainBean.getEnvSetup().getSmokDatasource(m))
                .filter(s->s!=null).collect(Collectors.toUnmodifiableList());
        List<DbBeanWithFailOverInterface> chain = new LinkedList<>();
        DbBeanWithFailOverInstance last = null;
        for(int i=0; i< source.size(); i++){
            DbBeanWithFailOverInstance dbi = new DbBeanWithFailOverInstance(mainBean.getEnvSetup(), source.get(i), names.get(i));
            try {
                dbi.connect();
                if (last != null) {
                    last.setNext(dbi);
                }
                last = dbi;
                chain.add(dbi);
            } catch (Exception e){
                LOGGER.warn("Failed to connect to datasource "+source);
            }
        }
        return chain.get(0);
    }

    public PreparedStatement  getCassPreparedStatement(String sql){
        return mainBean.getCassPreparedStatement(sql);
    }

  public void preloadEntities(){
       mainBean.preloadEntities();
  }

  protected static synchronized DbBeanInstance getSingletonLazy(Map<String, String> inMap){
      if (mainBean==null){
          mainBean = new DbBeanInstance( new DBEnvSetup(new CassandraState(false), inMap));
      }
      return mainBean;
  }

  //  protected static synchronized DbBeanInterface getSingletonLazy(){
  //      if (mainBean==null){
  //          mainBean = new DbBeanInstance(new DBEnvSetup( new CassandraState(false), new HashMap<>()));
  ///     }
  //      return mainBean;
  //  }


  //  protected static synchronized DbBeanInterface getSingletonLazy(String url, String driver, String user, String pass){
  //      if (mainBean==null){
  //          mainBean = new DbBeanInstance(url, driver, user, pass);
  //      }
  //      return (DbBeanInterface) mainBean;
  //  }



  public boolean connect() throws ClassNotFoundException,SQLException{
          if (mainBean==null) {
              mainBean = new DbBeanInstance();
          }
          if (mainBean.getCqlTemplate()==null || mainBean.getJdbcTemplate()==null) {
              return mainBean.connect();
          }
          return false;
        }




  public void close() throws SQLException{
        if (mainBean==null){ return; }
        mainBean.close();
        mainBean=null;
  }


  @Deprecated
  public ResultSet execSQL(String sql) throws SQLException{
        return mainBean.execSQL(sql);
  }

  public void execSQLNoResult(String sql, Object... params) throws SQLException {
        mainBean.execSQL( sql, params); //execSQlNoResult(String sql, params);
  }


  public ResultSet execSQL(String sql, Object... params) throws SQLException{

      return mainBean.execSQL(sql, params);

  }

  public boolean existsSQL(String sql) throws SQLException {
   //   Statement s = dbCon.createStatement();
   //   ResultSet r = s.executeQuery(sql);
        return mainBean.existsSQL(sql);
  }

  public int updateSQL(String sql, Object... params) throws SQLException{
    return mainBean.updateSQL(sql, params);
  }

  public MajoranaAnnotationRepository getRepo(Class beanClass){
        return mainBean.getRepo(beanClass);
  }

    public MajoranaAnnotationRepository getRepo(String entityName){
        return mainBean.getRepo(entityName);
    }

    // List Tables

    public List<Object> getBeansNP( Class beanClass,  String table,  String sql1, String[] paramNames, Object[] params) throws SQLException {
        return mainBean.getBeansNP( beanClass, table ,sql1, paramNames, params);
    }


    public List<Object> getBeansNPWithSelectClause( Class beanClass,  String table,  String sql1, String[] paramNames, Object[] params) throws SQLException {
        return mainBean.getBeansNPWithSelectClause( beanClass, table, sql1, paramNames, params);
    }

    public List<Object> getBeansNPWithSelectClause( Class<? extends BaseDistillerEntity> beanClass,    String sql1, String[] paramNames, Object[] params) throws SQLException {
        return mainBean.getBeansNPWithSelectClause( beanClass, getTableName(beanClass) ,sql1, paramNames, params);
    }

    public List<Object> getBeans( Class beanClass,  String table,  String sql1,  Object[] params) throws SQLException {
        return mainBean.getBeans( beanClass, table,sql1, params);
    }

    public List<Object> getBeans( Class<? extends BaseDistillerEntity> beanClass, String sql1,  Object[] params) throws SQLException {
        return mainBean.getBeans( beanClass, getTableName(beanClass),sql1, params);
    }

    public Object[] getBeansArrayNP(Class beanClass, String table, String sql,  String[] paramNames, Object[] params) throws SQLException {
        return mainBean.getBeansNP(beanClass, table, sql, paramNames, params).toArray(new Object[]{});
    }

    public Object[] getBeansArrayNP(Class<? extends BaseDistillerEntity> beanClass, String sql,  String[] paramNames, Object[] params) throws SQLException {
            return mainBean.getBeansNP(beanClass, getTableName(beanClass), sql, paramNames, params).toArray(new Object[]{});
    }

    public Object[] getBeansArrayNPWithSelectClause(Class beanClass, String table, String sql, String[] paramNames, Object[] params) throws SQLException {
        return mainBean.getBeansNPWithSelectClause(beanClass, table, sql, paramNames, params).toArray(new Object[]{});
    }

    public Object[] getBeansArrayNPWithSelectClause(Class<? extends BaseDistillerEntity> beanClass, String sql,  String[] paramNames, Object[] params) throws SQLException {
            return mainBean.getBeansNPWithSelectClause(beanClass, getTableName(beanClass), sql, paramNames, params).toArray(new Object[]{});
    }

    public Object[] getBeansArray(Class<? extends BaseDistillerEntity> beanClass, String sql,  Object[] params) throws SQLException {
        return mainBean.getBeans(beanClass, getTableName(beanClass), sql,  params).toArray(new Object[]{});
    }

    private String getTableName(Class<? extends BaseDistillerEntity> beanClass){
        try {
            BaseDistillerEntity inst = beanClass.newInstance();
            return inst.getTableName();
        } catch (InstantiationException | IllegalAccessException e){
            LOGGER.error("Cannot Instantiate "+beanClass.getCanonicalName());
        }
        return "-- NO TABLE FOUND --";
    }


//    public Object[] getBeansArrayNP(Class<? extends BaseDistillerEntity> beanClass, String sql, String[] paramNames, Object[] params) throws SQLException {
//        return getBeans((Class) beanClass, getTableName(beanClass), sql, paramNames, params).toArray(new Object[]{});
//    }


//    public Object[] getBeansArray(Class<? extends BaseDistillerEntity> beanClass, String sql, Object[] params) throws SQLException {
//          return getBeans(beanClass, getTableName(beanClass), sql, params).toArray(new Object[]{});
//    }

//    public Object[] getBeansArrayWithSelectClause(Class<? extends BaseDistillerEntity> beanClass, String sql, Object[] params) throws SQLException {
//            return getBeansArrayWithSelectClause(beanClass, getTableName(beanClass), sql, params).toArray(new Object[]{});
//    }


    public Object getBeanNP( Class beanClass,  String table, String sql, String[] paramNames, Object[] params) throws SQLException {
        List<Object> a = mainBean.getBeansNP(beanClass, table, sql, paramNames, params);
        if (a.size()>0){ return a.get(0); }
        return null;
    }

    public Object getBeanNPWithSelectClause(Class beanClass, String table, String sql, String[] paramNames, Object[] params) throws SQLException {
        return mainBean.getBeansNPWithSelectClause( beanClass, table, sql, paramNames, params).get(0);
    }

    public Object getBeanNPWithSelectClause( Class<? extends BaseDistillerEntity> beanClass, String sql, String[] paramNames, Object[] params) throws SQLException {
        List<Object> a = mainBean.getBeansNPWithSelectClause(beanClass, getTableName(beanClass), sql, paramNames, params);
        if (a.size()>0){ return a.get(0); }
        return null;
    }

    public List<Object> getBeansNP(Class<? extends BaseDistillerEntity> beanClass, String sql, String[] paramNames, Object[] params) throws SQLException {
        return mainBean.getBeansNP(beanClass, getTableName(beanClass), sql, paramNames, params);
    }

 //   @Override
 //   public List<Object> getBeansNPWithSelectClause(Class beanClass, String sql, String[] paramNames, Object[] params) throws SQLException {
 //       return mainBean.getBeansNPWithSelectClause(beanClass, getTableName(beanClass), sql, paramNames, params);
//    }

    public Object getBean( Class beanClass,  String table, String sql, Object[] params) throws SQLException {
        List<Object> a = mainBean.getBeans(beanClass, table, sql,  params);
        if (a.size()>0){ return a.get(0); }
        return null;
    }

    public Map<String, Object> getParamMap(String named[], Object params[]){
        return mainBean.getParamMap(named, params);
    }




    public int deleteBeans( Class beanClass,  String table,  String[] paramNames, Object[] params) throws SQLException {

       return mainBean.deleteBeans( beanClass, table, paramNames, params);

    }

    public String getFields(Class c){
        return mainBean.getRepo(c).getSqlFieldString();
    }

    public String getFields(Class c, String prefix){
        return mainBean.getRepo(c).getSqlFieldStringWithPrefix(prefix);
    }


    public MultiId storeBean(    BaseDistillerEntity bde) throws SQLException {

       return mainBean.storeBean( bde);

    }

    public MultiId updateBean( MultiId mid,  BaseDistillerEntity bde) throws SQLException {

       return mainBean.updateBean(mid, bde);

    }

    public UUID getUUID(String s){
      return UUID.fromString(s);
    }

    public MultiId deleteBeanById( MultiId mid,  BaseDistillerEntity bde) throws SQLException {

       return mainBean.deleteBeanById(mid, bde);

    }

    public MultiId deleteBeanByParams( MultiId mid,  BaseDistillerEntity bde) throws SQLException {

       return mainBean.deleteBeanByParams(mid, bde);

    }

    protected class FloatClass {
        private float f;

        public float getF() {
            return f;
        }

        public void setF(float f) {
            this.f = f;
        }
    }


    public Object getBean(Class beanClass, String sql, Object[] params) throws SQLException {
        return mainBean.getBean(beanClass, sql, params);
    }


    public Object getBeanNP(Class<? extends BaseDistillerEntity> beanClass, String sql, String[] paramNames, Object[] params) throws SQLException {
        return mainBean.getBeanNP( beanClass, getTableName(beanClass), sql, paramNames, params);
    }



    public List getBeans(Class beanClass, String table, String sql, String... params) throws SQLException {

        return mainBean.getBeans(beanClass, table, sql, params);

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
            } else {
                ps.setString(i++, (String) arg);
            }
        }
    return i;
}


  public float readFloat( Class beanClass, String sql, List<Object> params) throws Exception {
        return mainBean.readFloat(beanClass, sql, params);
  }

  public long writeJavaObject( Object object, String sql) throws Exception {

    return mainBean.writeJavaObject(object, sql);
	  }

	  public Object readJavaObject( String sql, Object[] params) throws Exception {
       return mainBean.readJavaObject( sql, params);
	  }


}
