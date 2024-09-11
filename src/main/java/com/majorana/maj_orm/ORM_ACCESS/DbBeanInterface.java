
package com.majorana.maj_orm.ORM_ACCESS;

import com.majorana.maj_orm.ORM.MajoranaAnnotationRepository;
import com.majorana.maj_orm.ORM.MajoranaRepositoryField;
import com.majorana.maj_orm.ORM.BaseMajoranaEntity;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import java.sql.*;
import java.util.*;

import static org.mockito.Mockito.mock;

;
;

public interface DbBeanInterface {

// Bean interface

    public PreparedStatement  getCassPreparedStatement(String sql);

  public void preloadEntities();


  public boolean connect() throws ClassNotFoundException,SQLException;

  public void close() throws SQLException;

  @Deprecated
  public ResultSet execSQL(String sql) throws SQLException;


  public ResultSet execSQL(String sql, Object... params) throws SQLException;

    public boolean existsSQL(String sql) throws SQLException;

    public int updateSQL(String sql, Object... params) throws SQLException;
  public MajoranaAnnotationRepository getRepo(Class beanClass);

  public MajoranaAnnotationRepository getRepo(String entityName);



    public Map<String, Object> getParamMap(String named[], Object params[]);

    public int deleteBeans( Class beanClass,  String table,  String[] paramNames, Object[] params) throws SQLException;

    public MultiId storeBean(    BaseMajoranaEntity bde) throws SQLException;

    public MajoranaRepositoryField getIdField(Class beanClass);

    public MultiId updateBean( MultiId mid,  BaseMajoranaEntity bde) throws SQLException;

    public MultiId deleteBeanById( MultiId mid,  BaseMajoranaEntity bde) throws SQLException ;

    public MultiId deleteBeanByParams( MultiId mid,  BaseMajoranaEntity bde) throws SQLException ;


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