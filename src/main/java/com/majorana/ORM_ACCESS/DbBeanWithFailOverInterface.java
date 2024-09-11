package com.majorana.ORM_ACCESS;

import com.majorana.ORM.BaseMajoranaEntity;

import java.sql.SQLException;
import java.util.List;

public interface DbBeanWithFailOverInterface {

    // Singlar no table
    public Object getBean(Class<? extends BaseDistillerEntity> beanClass, String sql, Object[] params) throws SQLException;

    public Object getBeanNP( Class<? extends BaseDistillerEntity> beanClass,  String sql,  String[] paramNames, Object[] params) throws SQLException;

    // Singular Get with Table and Params
    public Object getBean( Class beanClass,  String table, String sql, Object[] params) throws SQLException;

    public Object getBeanNP( Class beanClass,  String table, String sql,  String[] paramNames, Object[] params) throws SQLException;

    public Object getBeanNPWithSelectClause( Class beanClass,  String table, String sql,  String[] paramNames, Object[] params) throws SQLException;

    // List no table
    public List<Object> getBeansNP(Class<? extends BaseDistillerEntity> beanClass, String sql, String[] paramNames, Object[] params) throws SQLException;

    public List<Object> getBeansNPWithSelectClause(Class<? extends BaseDistillerEntity> beanClass, String sql, String[] paramNames, Object[] params) throws SQLException;

    public List<Object> getBeans(Class<? extends BaseDistillerEntity> beanClass, String sql, Object[] params) throws SQLException;




}
