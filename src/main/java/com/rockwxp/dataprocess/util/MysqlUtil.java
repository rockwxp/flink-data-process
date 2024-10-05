package com.rockwxp.dataprocess.util;

import com.rockwxp.dataprocess.common.AppCommon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author rock
 * @date 2024/10/2 08:44
 */
public class MysqlUtil {
    public static Connection getConnection() {
        Connection connection = null;
        try {
             connection = DriverManager.getConnection(AppCommon.MYSQL_URL, AppCommon.MYSQL_USERNAME, AppCommon.MYSQL_PASSWD);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return connection;
    }
}
