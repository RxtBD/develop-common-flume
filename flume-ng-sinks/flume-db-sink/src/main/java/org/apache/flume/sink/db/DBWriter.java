/**
 * FileName: DBWriter
 * Author:   Ren Xiaotian
 * Date:     2018/9/11 17:16
 */

package org.apache.flume.sink.db;

import org.apache.flume.conf.Configurable;

import java.sql.PreparedStatement;

public interface DBWriter extends Configurable {

    public abstract void insert(PreparedStatement preparedStatement, String columnName, String param) throws Exception;

}
