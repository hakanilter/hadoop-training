package com.devveri.hive.jdbc;

import java.sql.*;

public class HiveJdbcExample {

    private static final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String JDBC_URL = "jdbc:hive2://localhost:10000/test";
    private static final String JDBC_USER = "";
    private static final String JDBC_PASS = "";

    public void test() throws ClassNotFoundException, SQLException {
        // load class
        Class.forName(JDBC_DRIVER);

        try (Connection con = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASS);
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery("select * from test.nyse limit 10")) {
            while (rs.next()) {
                System.out.println(String.format("%s\t%s\t%f",
                        rs.getString("exchange"),
                        rs.getString("date"),
                        rs.getDouble("stock_price_open")));
            }
        }
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        new HiveJdbcExample().test();
    }

}
