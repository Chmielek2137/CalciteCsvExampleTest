package org.example;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.csv.*;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Sources;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        try {
            testPrepared();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void printProperties(Properties prop)
    {
        for (Object key: prop.keySet()) {
            System.out.println(key + ": " + prop.getProperty(key.toString()));
        }
    }

    static void testPrepared() throws SQLException {
        final Properties properties = new Properties();
        properties.setProperty("caseSensitive", "true");
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", properties)) {
            final CalciteConnection calciteConnection =
                    connection.unwrap(CalciteConnection.class);

            System.out.println("prop: " + connection.getMetaData().isReadOnly());

//            SchemaPlus rootSchema = calciteConnection.getRootSchema();
//            final DataSource ds = JdbcSchema.dataSource("jdbc:postgresql://localhost:5432/test1",
//                    "org.postgresql.Driver", "postgres", "postgres");
//            rootSchema.add("DB1", JdbcSchema.create(rootSchema, "DB1", ds, null, null));
//
//            Statement stmt = connection.createStatement();
//            ResultSet rs = stmt.executeQuery("select * from db1.\"test_table1\"");
//
//            while (rs.next()) {
//                System.out.println(rs.getString(1) + '=' + rs.getString(2));
//            }

            final Schema schema =
                    CsvSchemaFactory.INSTANCE
                            .create(calciteConnection.getRootSchema(), null,
                                    ImmutableMap.of("directory",
                                            resourcePath("sales"), "flavor", "scannable"));
            calciteConnection.getRootSchema().add("MYSCHEMA", schema);
            final String sql = "select \"station_id\", \"name\" from \"MYSCHEMA\".\"STATION\"";
            final PreparedStatement statement2 =
                    calciteConnection.prepareStatement(sql);

            final ResultSet resultSet1 = statement2.executeQuery();
            while (resultSet1.next()) {
                System.out.println(resultSet1.getString(1) );
            }
        }
    }

    private static String resourcePath(String path) {
        return Sources.of(Main.class.getResource("/" + path)).file().getAbsolutePath();
    }
}