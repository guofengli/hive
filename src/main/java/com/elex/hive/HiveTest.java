package com.elex.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveTest {
	public static String conStr = "jdbc:mysql://namenode:3306/metastore?useUnicode=true&amp;characterEncoding=UTF-8";
	public static String driverStr = "org.apache.hive.jdbc.HiveDriver";
	public static String dataUser = "hive";
	public static String dataPass = "hiveuser";
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		try {
			Class.forName(driverStr);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Connection con = DriverManager.getConnection(conStr,dataUser, dataPass);
		Statement stmt = con.createStatement();
		
	}

}
