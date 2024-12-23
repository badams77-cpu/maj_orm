package com.majorana.maj_orm.ORM;

import jakarta.persistence.Column;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class MajoranaRepositoryField {

    private String name;
    private Field field;
    private Method getter;
    private Method setter;
    private String dbColumn;
    private Column columnAnnotation;
    private Class valueType;
    private int varcharSize;

    private boolean isTransient;

    private boolean isId;

    private boolean isAltId;

    private boolean populatedCreated;

    private boolean populatedUpdated;

    private boolean updateable;

    private boolean nullable;

    public int getVarcharSize() {
        return varcharSize;
    }

    public void setVarcharSize(int varcharSize) {
        this.varcharSize = varcharSize;
    }

    public MajoranaRepositoryField(){

    }

    public int getVarcharSize() {
        return varcharSize;
    }

    public void setVarcharSize(int varcharSize) {
        this.varcharSize = varcharSize;
    }

    public boolean isAltId() {
        return isAltId;
    }

    public void setAltId(boolean altId) {
        isAltId = altId;
    }

    public boolean isTransient() {
        return isTransient;
    }

    public void setTransient(boolean aTransient) {
        isTransient = aTransient;
    }

    public boolean isId() {
        return isId;
    }

    public void setId(boolean id) {
        isId = id;
    }

    public boolean checkFields(){
        return name!=null && field!=null && getter!=null && setter!=null && dbColumn!=null && valueType!=null && columnAnnotation!=null;
    }

    public boolean isPopulatedCreated() {
        return populatedCreated;
    }

    public void setPopulatedCreated(boolean populatedCreated) {
        this.populatedCreated = populatedCreated;
    }

    public boolean isUpdateable() {
        return updateable;
    }

    public void setUpdateable(boolean updateable) {
        this.updateable = updateable;
    }

    public boolean isPopulatedUpdated() {
        return populatedUpdated;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public void setPopulatedUpdated(boolean populatedUpdated) {
        this.populatedUpdated = populatedUpdated;
    }

    public String getMissing(){
        StringBuffer buf = new StringBuffer();
        if (name==null){ buf.append("name "); }
        if (field==null){ buf.append("field "); }
        if (getter==null){ buf.append("getter "); }
        if (setter==null){ buf.append("setter "); }
        if (valueType==null){ buf.append("valueType "); }
        if (columnAnnotation==null){ buf.append("Column Annotation"); }
        return buf.toString();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }

    public Method getGetter() {
        return getter;
    }

    public void setGetter(Method getter) {
        this.getter = getter;
    }

    public Method getSetter() {
        return setter;
    }

    public void setSetter(Method setter) {
        this.setter = setter;
    }

    public String getDbColumn() {
        return dbColumn;
    }

    public void setDbColumn(String dbColumn) {
        this.dbColumn = dbColumn;
    }

    public Class getValueType() {
        return valueType;
    }

    public void setValueType(Class valueType) {
        this.valueType = valueType;
    }

    public Column getColumnAnnotation() {
        return columnAnnotation;
    }

    public void setColumnAnnotation(Column columnAnnotation) {
        this.columnAnnotation = columnAnnotation;
    }

    @Override
    public String toString() {
        return "MajoranaRepositoryField{" +
                "name='" + name + '\'' +
                ", field=" + field +
                ", getter=" + getter +
                ", setter=" + setter +
                ", dbColumn='" + dbColumn + '\'' +
                ", columnAnnotation=" + columnAnnotation +
                ", valueType=" + valueType +
                ", isTransient=" + isTransient +
                ", isId=" + isId +
                ", isAltId=" + isAltId +
                ", populatedCreated=" + populatedCreated +
                ", populatedUpdated=" + populatedUpdated +
                ", updateable=" + updateable +
                ", nullable=" + nullable +
                '}';
    }
}
