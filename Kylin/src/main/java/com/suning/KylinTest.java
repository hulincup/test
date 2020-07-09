package com.suning;

import java.sql.*;

/**
 * @author lynn
 */
public class KylinTest {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        //KYLIN JDBC驱动
        String KYLIN_DRIVER = "org.apache.kylin.jdbc.Driver";

        //Kylin_URL
        String KYLIN_URL = "jdbc:kylin://bd1301:7070/emp_demo";

        //Kylin的用户名
        String KYLIN_USER = "ADMIN";

        //Kylin的密码
        String KYLIN_PASSWD = "KYLIN";

        //1.获取驱动
        Class.forName(KYLIN_DRIVER);
        //2.获取连接
        Connection connection = DriverManager.getConnection(KYLIN_URL, KYLIN_USER, KYLIN_PASSWD);

        //3.准备Sql
        String sql = "select sum(sal) from emp group by deptno";

        //4.获取prepareStatement预编译对象
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //5.执行查询
        ResultSet resultSet = preparedStatement.executeQuery();

        //6.打印遍历
        while (resultSet.next()){
            System.out.println(resultSet.getInt(1));
        }
    }
}
