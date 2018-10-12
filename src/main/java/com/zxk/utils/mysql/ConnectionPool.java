package com.zxk.utils.mysql;

import java.sql.Connection;
import java.util.LinkedList;

/**
 * Describe: Mysql
 *
 * @author : ZhuXiaokang
 * @mail : xiaokang.zhu@pactera.com
 * @date : 2018/6/28 17:58
 * Attention:
 * Modify:
 */
public class ConnectionPool {

    private static LinkedList<Connection> connectionQueue;

    static String MYSQL_IP = System.getProperty("mysql_ip");

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection() {
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<Connection>();
                for (int i = 0;i < 5;i ++) {
                    Connection conn = java.sql.DriverManager.getConnection(
                            "jdbc:mysql://"+MYSQL_IP+":3316/sparktest?characterEncoding=utf8",
                            "root",
                            "hello@java"
                    );
                    connectionQueue.push(conn);
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }
}
