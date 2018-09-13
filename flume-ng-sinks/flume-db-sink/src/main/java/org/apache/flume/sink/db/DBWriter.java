/**
 * FileName: DBWriter
 * Author:   Ren Xiaotian
 * Date:     2018/9/11 17:16
 */

package org.apache.flume.sink.db;

import java.sql.PreparedStatement;

public interface DBWriter {

    public abstract void insert(PreparedStatement preparedStatement, String columnName, String param) throws Exception;

}
