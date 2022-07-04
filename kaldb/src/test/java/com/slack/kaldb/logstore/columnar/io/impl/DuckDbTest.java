package com.slack.kaldb.logstore.columnar.io.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class DuckDbTest {

  public static void main(String[] args) throws SQLException {
    try {
      Class.forName("nl.cwi.da.duckdb.DuckDBDriver");
    } catch (ClassNotFoundException e) {
    }

    // Create spans table
    String createSpansTableCmd =
        "CREATE TABLE spans(id VARCHAR, traceId VARCHAR, parentId VARCHAR, serviceName VARCHAR, "
            + "name VARCHAR, timestamp INT, durationMicros BIGINT, "
            + "strField1 VARCHAR, strField2 VARCHAR, strField3 VARCHAR, strField4 VARCHAR, strField5 VARCHAR, "
            + "intField1 INT, intField2 INT, intField3 INT, intField4 INT, intField5 INT, "
            + "longField1 BIGINT, longField2 BIGINT, longField3 BIGINT, longField4 BIGINT, longField5 BIGINT,"
            + "boolField1 BOOL, boolField2 BOOL);\n";

    Connection con = DriverManager.getConnection("jdbc:duckdb:");
    Statement st = con.createStatement();
    st.execute(createSpansTableCmd);

    // Insert a few rows
    Map<String, String> fieldMapper = new HashMap<>();

    // Query the data.
  }
}
