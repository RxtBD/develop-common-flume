/**
 * FileName: AbstractDBWriter
 * Author:   Ren Xiaotian
 * Date:     2018/9/11 17:57
 */

package org.apache.flume.sink.db;

import java.sql.PreparedStatement;

public class AbstractDBWriter implements DBWriter {

    @Override
    public void insert(PreparedStatement preparedStatement, String columnName, String param) throws Exception {

    }
}
