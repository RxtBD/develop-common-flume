/**
 * FileName: OracleWriter
 * Author:   Ren Xiaotian
 * Date:     2018/9/11 17:55
 */

package org.apache.flume.sink.db;

import java.sql.PreparedStatement;

/**
 * Oracle写入器
 */
public class OracleWriter extends AbstractDBWriter{
    @Override
    public void insert(PreparedStatement preparedStatement, String columnName, String param) throws Exception {
        super.insert(preparedStatement, columnName, param);

        //如果拿到的列数和指定的列数一致，再进行入库操作
        if (param.split(",").length == columnName.split(",").length) {
            String[] arrs = param.split(",");
            int pos = 1;
            for (String s : arrs) {
                preparedStatement.setString(pos, s);
                pos++;
            }
            preparedStatement.execute();//执行插入操作
        }

    }
}
