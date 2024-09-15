
package com.majorana.maj_orm.ORM_ACCESS;

import com.majorana.maj_orm.ORM.MajoranaAnnotationRepository;
import com.majorana.maj_orm.ORM.MajoranaRepositoryField;
import com.majorana.maj_orm.ORM.BaseMajoranaEntity;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

;
;

public interface DbBeanInstInterface {

// Bean interface

//    public PreparedStatement  getCassPreparedStatement(String sql);

  public void preloadEntities();


  public boolean connect() throws ClassNotFoundException,SQLException;

  public void close() throws SQLException;

  @Deprecated
  public ResultSet execSQL(String sql) throws SQLException;


  public ResultSet execSQL(String sql, Object... params) throws SQLException;

    public boolean existsSQL(String sql) throws SQLException;

    public int updateSQL(String sql, Object... params) throws SQLException;


    public int updateBeanNP(BaseMajoranaEntity bde, String[] sqlWhereParam, Object ids[],  String[] paramNames, Object[] params) throws SQLException;

    public MajoranaAnnotationRepository getRepo(Class beanClass);



    public Map<String, Object> getParamMap(String named[], Object params[]);

    public int deleteBeans( Class beanClass,  String table,  String[] paramNames, Object[] params) throws SQLException;

    public MultiId storeBean(    BaseMajoranaEntity bde) throws SQLException;

    public MajoranaRepositoryField getIdField(Class beanClass);

    public MultiId updateBean( MultiId mid,  BaseMajoranaEntity bde) throws SQLException;

    public MultiId deleteBeanById( MultiId mid,  BaseMajoranaEntity bde) throws SQLException ;

    public MultiId deleteBeanByParams( MultiId mid,  BaseMajoranaEntity bde) throws SQLException ;

    // Singular Get with Table and Params
    public Object getBean( Class beanClass,  String table, String sql, Object[] params) throws SQLException;

    public Object getBeanNP( Class beanClass,  String table, String sql,  String[] paramNames, Object[] params) throws SQLException;

    public Object getBeanNPWithSelectClause( Class beanClass,  String table, String sql,  String[] paramNames, Object[] params) throws SQLException;

    // List  table
    public List<Object> getBeansNP(Class beanClass,  String table,String sql, String[] paramNames, Object[] params) throws SQLException;

    public List<Object> getBeansNPWithSelectClause(Class beanClass, String table, String sql, String[] paramNames, Object[] params) throws SQLException;

    public List<Object> getBeans(Class beanClass, String table, String sql, Object[] params) throws SQLException;



    class FloatClass {
        private float f;

        public float getF() {
            return 0;
        }

        public void setF(float f) {
            this.f = f;
        }
    }


  public float readFloat( Class beanClass, String sql, List<Object> params) throws Exception;

  public Object readJavaObject( String sql, Object[] params) throws Exception;

    public long writeJavaObject( Object object, String sql) throws Exception;

}