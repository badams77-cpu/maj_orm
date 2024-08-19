package Distiller.DBs;

import java.util.Objects;

/**
 * Unique name of a datasource to use
 */

public class SmokDatasourceName {

    private final String DataSourceName;

    public SmokDatasourceName(String name){
        this.DataSourceName =name;
    }

    public String getDataSourceName() {
        return DataSourceName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SmokDatasourceName that = (SmokDatasourceName) o;
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
