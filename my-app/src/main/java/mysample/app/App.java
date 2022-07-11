package mysample.app;

import java.sql.*;


public class App {

    public static void main(String[] args) throws Exception {
        // Open a connection

        String token = "dapi***";
        String url = "jdbc:databricks://***.databricks.com:443/default;" +
                "transportMode=http;ssl=1;AuthMech=3;httpPath=sql/protocolv1/o/***;" +
                "UID=token;" +
                "PWD=" + token;

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM moe_derakhshani_dbtest.tabletest");) {
            // Extract data from result set
            while (rs.next()) {
                // Retrieve by column name
                System.out.print("ID: " + rs.toString());

            }
        }
    }
}



