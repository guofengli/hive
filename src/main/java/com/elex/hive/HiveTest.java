package com.elex.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class HiveTest {
	public static String urlStr = "jdbc:hive2://namenode:10000/default";
	public static String driverStr = "org.apache.hive.jdbc.HiveDriver";
	public static String dataUser = "hive";
	public static String dataPass = "hiveuser";
	
	public static Connection getCon(){
		Connection con = null;
		try {
			Class.forName(driverStr);
			con = DriverManager.getConnection(urlStr, dataUser, dataPass);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return con;
	}
	
	public static boolean createTable(Connection con, String tableName, Map<String, String> maps){
		String createTableStr = "create table " + tableName + " (";
		int mapSize = maps.size();
		boolean result = false;
		for(Map.Entry<String, String> map:maps.entrySet()){
			if(--mapSize != 0) {
			createTableStr += map.getKey() + " " + map.getValue() + ",";
			} else{
				createTableStr += map.getKey() + " " + map.getValue() + ")"; 
			}
		}
		createTableStr += "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE";
		try {
			Statement stmt = con.createStatement();
			result = stmt.execute(createTableStr);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	public static boolean dropTable(Connection con, String tableName){
		boolean result = false;
		try {
			Statement stmt = con.createStatement();
			result = stmt.execute("drop table if exists " + tableName);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	public static void selectTable(Connection con, String sql){
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while(rs.next()){
				String id = rs.getString(1);
				String name = rs.getString(2);
				String tfidf = rs.getString(3);
				System.out.println(id + "\t" + name + "\t" + tfidf);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static boolean loadData(Connection con, String sql){
		boolean result = false;
		try {
			Statement stmt = con.createStatement();
			result = stmt.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	
	
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		Connection con = getCon();
		Statement stmt = con.createStatement();
		Map<String, String> maps = new HashMap<String, String>();
		maps.put("word", "String");
		maps.put("user", "String");
		maps.put("tfidf", "String");
//		System.out.println(createTable(con, "tfidf", maps));
		String sql  = "select * from tfidf";
		selectTable(con, sql);
		
		sql = "load data local inpath 'E:/work/tfidf' overwrite into table tfidf";
//		System.out.println(loadData(con, sql));
		
		sql = "show tables";
		ResultSet res = stmt.executeQuery(sql);
		while(res.next()){
			System.out.println(res.getString(1));
		}
	}
	
	
}
