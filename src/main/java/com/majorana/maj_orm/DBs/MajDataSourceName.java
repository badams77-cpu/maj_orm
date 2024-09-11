package com.majorana.maj_orm.DBs;

import java.util.Objects;

/**
 * Unique name of a datasource to use
 */

public class MajDataSourceName {

    private final String DataSourceName;

    public MajDataSourceName(String name){
        this.DataSourceName =name;
    }

    public String getDataSourceName() {
        return DataSourceName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MajDataSourceName that = (MajDataSourceName) o;
        return Objects.equals(DataSourceName, that.DataSourceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(DataSourceName);
    }

    @Override
    public String toString() {
        return "MajDatasourceName{" +
                "DataSourceName='" + DataSourceName + '\'' +
                '}';
    }
}
