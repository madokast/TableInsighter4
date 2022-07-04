package com.sics.rock.tableinsight4.procedure.load;

import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;


public class FDerbyJdbcTableLoaderTest extends FTableInsightEnv {

    private static final String JDBC_URL = "jdbc:derby:derbyDB;create=true";
    private static final String DRIVER_CLASS = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String USER = "";
    private static final String PASSWORD = "";

    @Test
    public void test_jdbc() {
        Map<String, String> options = config().jdbcTableLoadOptions;
        options.put("url", JDBC_URL);
        options.put("user", USER);
        options.put("password", PASSWORD);
        FTableLoader loader = new FTableLoader();
        loader.load("TEST_TABLE").show();
    }


    @Before
    public void prepare() throws ClassNotFoundException, SQLException {
        Class.forName(DRIVER_CLASS);
        try (
                Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
                Statement statement = conn.createStatement();
        ) {
            statement.execute("CREATE TABLE TEST_TABLE(row_id VARCHAR(50) PRIMARY KEY, name VARCHAR(50) NOT NULL, age INTEGER NOT NULL)");

            statement.executeUpdate("INSERT INTO TEST_TABLE VALUES('1', 'Zhao RX', 27) ");
            statement.executeUpdate("INSERT INTO TEST_TABLE VALUES('2', 'Tom', 28) ");
            statement.executeUpdate("INSERT INTO TEST_TABLE VALUES('3', 'Bob', 30) ");
        }
    }

    @After
    public void delete() {
        try (
                Connection connection = DriverManager.getConnection(
                        "jdbc:derby:;shutdown=true")
        ) {
            // reachable
            logger.debug("conn {}", connection);
        } catch (SQLException ignore) {
            // java.sql.SQLException: Derby system shutdown.
            // e.printStackTrace();
        }

        try {
            FileUtils.deleteDirectory(new File("derbyDB"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        new File("derby.log").delete();
    }


}