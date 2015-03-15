package org.neo4j.imports;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author mh
 * @since 01.03.15
 */
public class DataReader {
    private final Connection conn;

    public DataReader(Connection conn) {
        this.conn = conn;
    }

    public   ResultSet readTableData(TableInfo table) throws SQLException {
        // todo custom sql statements, limit, skip fields, fixed field order,
        return conn.createStatement().executeQuery("SELECT * from " + table.table);
    }
}
