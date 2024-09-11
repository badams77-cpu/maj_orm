
package com.majorana.ORM_ACCESS;

import com.majorana.ORM.MajoranaAnnotationRepository;
import com.majorana.ORM.MajoranaRepositoryField;
import com.majorana.ORM.BaseMajoranaEntity;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

;
;

public interface DbBeanGenericInterface<T extends BaseMajoranaEntity> {


  public MajoranaAnnotationRepository<T> getRepo();


    public List<T> getBeansNP( String sql, String[] paramNames, Object[] params);

    public List<T> getBeansNPWithSelectClause( String sql, String[] paramNames, Object[] params);

    public List<T> getBeans( String sql, Object[] params) throws SQLException;

    public T getBeanNP(  String sql, String[] paramNames, Object[] params) throws SQLException;

    public T getBeanNPWithSelectClause(  String sql, String[] paramNames, Object[] params) throws SQLException;

    public T[] getBeansArray( String sql,  Object[] params) throws SQLException;

    public T[] getBeansArrayNP( String sql, String[] paramNames, Object[] params) throws SQLException;

    public T[] getBeansArrayNPWithSelectClause( String sql, String[] paramNames, Object[] params) throws SQLException;

    public Map<String, Object> getParamMap(String named[], Object params[]);

    public String getFields();

    public String getFields(String pre);

    public String getIdFieldName();

    public int deleteBeans(  String[] paramNames, Object[] params) throws SQLException;

    public MultiId storeBean(  T bde) throws SQLException;

    public MultiId updateBean( MultiId mid,  T bde) throws SQLException;

    public int  updateAltIds( MultiId mid,  T bde) throws SQLException;


    public MultiId deleteBeanById( MultiId mid) throws SQLException, InstantiationException, IllegalAccessException ;

    public MultiId deleteBeanByParams( MultiId mid,  T bde) throws SQLException ;

    public MajoranaRepositoryField getIdField();

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


}