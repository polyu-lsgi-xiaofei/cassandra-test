package org.cassandra.sde.file;

import java.sql.Connection;
import java.sql.DriverManager;

public class SQLSeverConnector {
	public static void main(String[] args)
	{
		String driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		String dbURL = "jdbc:sqlserver://localhost:1433;DatabaseName=master";
		String userName = "sa";
		String userPwd = "869222";
		try {
			Class.forName(driverName);
			Connection dbConn = DriverManager.getConnection(dbURL, userName, userPwd);
			System.out.println("连接数据库成功");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.print("连接失败");
		}
	}
}
