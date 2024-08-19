package Distiller.ORM;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.Row;


import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class CassMockResultSet {

     /*
     * Mocks a SQL ResultSet.
     */

        private Map<String, Integer> columnIndices;
        private Object[][] data;
        private int rowIndex;

        public CassMockResultSet(com.datastax.oss.driver.api.core.cql.ResultSet crs){
            columnIndices = new LinkedHashMap<>();
            int cols = 1;
            for(ColumnDefinition cd : crs.getColumnDefinitions()){
                columnIndices.put(cd.getName().toString(), cols++);
            }
            List<Row> crows = crs.all();
            data = new Object[crows.size()][];
            int rows = 0;
            for(Row r : crows){
                Object[] or = new Object[cols];
                for(int j=0 ; j<cols; j++){
                    or[j] = r.getObject(j);
                }
                data[rows++] = or;
            }
            this.rowIndex = -1;
        }

    public CassMockResultSet(com.datastax.driver.core.ResultSet crs){
        columnIndices = new LinkedHashMap<>();
        int cols = 1;
        for( ColumnDefinitions.Definition cd : crs.getColumnDefinitions().asList()){
            columnIndices.put(cd.getName().toString(), cols++);
        }
        List<com.datastax.driver.core.Row> crows = crs.all().stream().collect(Collectors.toList());
        data = new Object[crows.size()][];
        int rows = 0;
        for(com.datastax.driver.core.Row r : crows){
            Object[] or = new Object[cols];
            for(int j=0 ; j<cols; j++){
                or[j] = r.getObject(j);
            }
            data[rows++] = or;
        }
        this.rowIndex = -1;
    }

        public CassMockResultSet(final String[] columnNames, final Object[][] data) {
            this.columnIndices = IntStream.range(0, columnNames.length).boxed()
                    .collect(Collectors.toMap(k -> columnNames[k], Function.identity(), (a, b) -> {
                        throw new RuntimeException("Duplicate column " + a);
                    }, LinkedHashMap::new));
            this.data = data;
            this.rowIndex = -1;
        }

        public ResultSet buildMock() throws SQLException {
            final ResultSet rs = mock(ResultSet.class);

            // mock rs.next()
            doAnswer(invocation -> {
                rowIndex++;
                return rowIndex < data.length;
            }).when(rs).next();

            // mock rs.getString(columnName)
            doAnswer(invocation -> {
                String columnName = invocation.getArgument(0);
                Integer columnIndex = columnIndices.get(columnName);
                return (String) data[rowIndex][columnIndex];
            }).when(rs).getString(anyString());

            // mock rs.getString(columnIndex)
            doAnswer(invocation -> {
                Integer index = invocation.getArgument(0);
                return (String) data[rowIndex][index - 1];
            }).when(rs).getString(anyInt());

            // mock rs.getInt(columnName)
            doAnswer(invocation -> {
                String columnName = invocation.getArgument(0);
                Integer columnIndex = columnIndices.get(columnName);
                return (Integer) data[rowIndex][columnIndex];
            }).when(rs).getInt(anyString());

            // mock rs.getObject(columnName)
            doAnswer(invocation -> {
                String columnName = invocation.getArgument(0);
                Integer columnIndex = columnIndices.get(columnName);
                return data[rowIndex][columnIndex];
            }).when(rs).getObject(anyString());

            // mock rs.getObject(columnIndex)
            doAnswer(invocation -> {
                Integer index = invocation.getArgument(0);
                return data[rowIndex][index - 1];
            }).when(rs).getObject(anyInt());

            final ResultSetMetaData rsmd = mock(ResultSetMetaData.class);

            // mock rsmd.getColumnCount()
            doReturn(columnIndices.size()).when(rsmd).getColumnCount();

            // mock rsmd.getColumnName(int)
            doAnswer(invocation -> {
                Integer index = invocation.getArgument(0);
                return columnIndices.keySet().stream().skip(index - 1).findFirst().get();
            }).when(rsmd).getColumnName(anyInt());

            // mock rs.getMetaData()
            doReturn(rsmd).when(rs).getMetaData();

            return rs;
        }

        /**
         * Creates the mock ResultSet.
         *
         * @param columnNames the names of the columns
         * @param data
         * @return a mocked ResultSet
         * @throws SQLException
         */
        public static ResultSet create(final String[] columnNames, final Object[][] data)
                throws SQLException {
            return new CassMockResultSet(columnNames, data).buildMock();
        }


}
