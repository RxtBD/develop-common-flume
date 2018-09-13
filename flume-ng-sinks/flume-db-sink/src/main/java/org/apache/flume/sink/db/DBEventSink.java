/**
 * FileName: DBEventSink
 * Author:   Ren Xiaotian
 * Date:     2018/9/11 17:31
 */

package org.apache.flume.sink.db;

import com.google.common.base.Preconditions;
import org.apache.flume.*;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DBEventSink extends AbstractSink implements org.apache.flume.conf.Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(DBEventSink.class);

    private DBWriter dbWriter;
    private DBWriterFactory dbWriterFactory;

    public DBWriterFactory getDbWriterFactory() {
        return dbWriterFactory;
    }

    private String dbType;    //数据库类型：MySQL or Oracle or DB2
    private String class4Name;    //数据库连接驱动
    private String charset;    //字符集
    private String url;
    private String tableName;
    private String username;
    private String password;
    private String columnName;    //列名，以逗号分隔

    private Connection connection;
    private PreparedStatement preparedStatement;

    public DBEventSink() {
        this(new DBWriterFactory());
    }

    public DBEventSink(DBWriterFactory dbWriterFactory) {
        this.dbWriterFactory = dbWriterFactory;
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;
        transaction.begin();

        while (true) {
            event = channel.take();
            if (event != null) {    //如果拿到了东西，则跳出循环，进行处理
                break;
            }
        }
        try {
            String body = new String(event.getBody(), "UTF-8");

            this.dbWriter.insert(preparedStatement, this.columnName, body);    //调用写入器，将数据写入到库中
            transaction.commit();    //提交事务
            return Status.READY;
        } catch (Throwable throwable) {
            transaction.rollback();
            if (throwable instanceof Error) {
                throw (Error) throwable;
            } else {
                throw new EventDeliveryException(throwable);
            }
        } finally {
            transaction.close();
            return Status.BACKOFF;
        }

    }

    /**
     * sinks启动，做两件事：
     * 1.根据dbType获得特定数据库的写入器
     * 2.生成connection并预编译SQL
     */
    @Override
    public synchronized void start() {
        super.start();
        try {
            this.dbWriter = this.dbWriterFactory.getWriter(this.dbType);    //根据dbType获得特定数据库的写入器

            Class.forName(this.class4Name);
            this.connection = DriverManager.getConnection(this.url, this.username, this.password);

            //拼装预编译的sql
            StringBuffer sql = new StringBuffer("insert into ");
            sql.append(tableName).append("(");
            sql.append(this.columnName).append(")");
            sql.append(" values(");

            String[] columnNum = this.columnName.split(",");
            for (int i = 0; i < columnNum.length; i++) {
                if (i < columnNum.length - 1) {
                    sql.append("?").append(" ,");
                } else {
                    sql.append("?)");
                }
            }

            this.preparedStatement = connection.prepareStatement(sql.toString());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();
        try {
            if (this.preparedStatement != null) {
                this.preparedStatement.close();
            }
            if (this.connection != null) {
                this.connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized Channel getChannel() {
        return super.getChannel();
    }

    /**
     * 读取配置文件，获取以下配置项
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        this.dbType = context.getString("dbType");
        Preconditions.checkNotNull(this.dbType, "dbType must bt set!!!");
        this.class4Name = context.getString("class4Name");
        Preconditions.checkNotNull(this.class4Name, "class4Name must be set!!!");

        // 获取字符集，如不指定，则默认UTF-8
        this.charset = context.getString("charset", "UTF-8");

        this.url = context.getString("url");
        Preconditions.checkNotNull(this.url, "url must be set!!!");
        this.tableName = context.getString("tableName");
        Preconditions.checkNotNull(this.tableName, "tableName must be set!!!");
        this.username = context.getString("username");
        Preconditions.checkNotNull(this.username, "username must be set!!!");
        this.password = context.getString("password");
        Preconditions.checkNotNull(this.password, "password must be set!!!");
        this.columnName = context.getString("columnName");
        Preconditions.checkNotNull(this.columnName, "columnName must be set!!!");
    }
}
