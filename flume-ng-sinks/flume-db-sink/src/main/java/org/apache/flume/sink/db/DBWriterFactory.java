/**
 * FileName: DBWriterFactory
 * Author:   Ren Xiaotian
 * Date:     2018/9/11 17:37
 */

package org.apache.flume.sink.db;

import java.io.IOException;

public class DBWriterFactory {

    static final String MySQLType = "MYSQL";
    static final String OracleType = "ORACLE";
    static final String DB2Type = "DB2";

    public DBWriter getWriter(String dbType) throws IOException {
        if (MySQLType.equalsIgnoreCase(dbType)) {
            return new MySQLWriter();
        }
        if (OracleType.equalsIgnoreCase(dbType)) {
            return new OracleWriter();
        }
        if (DB2Type.equalsIgnoreCase(dbType)) {
            return new DB2Writer();
        }
        throw new IOException("DB type " + dbType + " not supported");
    }

}
