package org.neo4j.imports;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author mh
 * @since 01.03.15
 */
public class DataReader {
    public static ResultSet readTableData(Connection conn, TableInfo table) throws SQLException {
        // todo custom sql statements, limit, skip fields, fixed field order,
        return conn.createStatement().executeQuery("SELECT * from " + table.table);
    }
}
