package mysample.app;

import com.databricks.client.jdbc.DataSource;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Sample {
    private String endpointURL;
    private String patToken;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();


    public static void main(String[] args) throws Exception {
        Sample sample = new Sample();
        sample.loadConfig();
        sample.runQuery();
    }

    private void runQuery() throws Exception {

        DataSource dataSource = getDataSource();
        log("opening a connection");
        try (Connection conn = dataSource.getConnection()) {

            int count = 5;
            log(String.format("running simple query %d times", count));
            // run SELECT 1+1 10 times
            for(int i = 0; i < count; i++) {
                Statement stmt = conn.createStatement();
                String query = "SELECT 1+1";
                log(String.format("executing query [%s]", query));
                ResultSet rs = stmt.executeQuery(query);

                while(rs.next()) {
                    log("fetch items");
                }

                rs.close();
                log("stmt.executeQuery completed");
            }

            // JDBC driver does not support cancelling the query on the same thread as smt.executeQuery blocks
            // to cancel the query, invoke smt.cancel() on a different thread.
            Statement smt = conn.createStatement();
            executorService.submit(() -> {
                try {
                    // wait for 6 seconds
                    Thread.sleep(6000);
                    // after that attempt to cancel the operation
                    smt.cancel();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            // long-running query
            String longRunningQuery = "Select id from range(1000000000) order by random() + 2 asc";
            try {
                log(String.format("executing query [%s]", longRunningQuery));
                ResultSet rs = smt.executeQuery(longRunningQuery);
            } catch (java.sql.SQLException e) {
                // upon cancellation smt.executeQuery() throws SQLException catch here
                // exception details and e.getMessage() indicates cancellation as the reason for exception
                e.printStackTrace();

            }
        }
    }

    private DataSource getDataSource() {
        // How to initialize Databricks JDBC Datasource
        DataSource dataSource = new com.databricks.client.jdbc.DataSource();
        // set the endpoint url: will be in the following format
        // jdbc:databricks://myaccount.cloud.databricks.com:443/default;httpPath=/sql/1.0/endpoints/123456
        // to enable logging ;LogLevel=6;LogPath=/Users/Moe.Derakhshani/tmp/simba-logs need to be appended to the endpoint
        // e.g., jdbc:databricks://myaccount.cloud.databricks.com:443/default;httpPath=/sql/1.0/endpoints/123456;LogLevel=6;LogPath=/Users/Moe.Derakhshani/tmp/simba-logs
        dataSource.setURL(endpointURL);
        System.out.println("endpoint: " + endpointURL);
        dataSource.setUserID("token");
        dataSource.setPassword(patToken);
        return dataSource;
    }

    private void loadConfig() throws Exception {
        try (InputStream input = new FileInputStream("/Users/Moe.Derakhshani/Desktop/demo/config.properties")) {
            Properties prop = new Properties();
            prop.load(input);
            endpointURL = prop.getProperty("endpointURL");
            patToken = prop.getProperty("patToken");
        }
    }

    private void log(String msg) {
        System.out.println(new Date() + ": " + msg);
    }
}
