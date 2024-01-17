package com.majorana.orm.MajORM.DBs;

import java.util.Objects;

public class MajDatasourceName {

    private final String DataSourceName;

    public MajDatasourceName(String name){
        this.DataSourceName =name;
    }

    public String getDataSourceName() {
        return DataSourceName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MajDatasourceName that = (MajDatasourceName) o;
        return Objects.equals(DataSourceName, that.DataSourceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(DataSourceName);
    }

    @Override
    public String toString() {
        return "SmokDatasourceName{" +
                "DataSourceName='" + DataSourceName + '\'' +
                '}';
    }
}
