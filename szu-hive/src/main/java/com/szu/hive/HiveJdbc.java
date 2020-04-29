package com.szu.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
* 通过JDBC驱动方式操作Hive
*/

public class HiveJdbc {

	/*
	* 获取数据库连接
	*/
	public static Connection getConn() {

		//定义连接hive的url
		String url = "jdbc:hive://192.168.133.139:10000/default";
		
		//定义驱动名称
		String driverName = "org.apache.hive.jdbc.HiveDriver";

		//定义数据库连接对象Connection
		Connection con = null;
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
		try {
			return DriverManager.getConnection(url);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return con;
	}


	/*
	* 释放连接资源
	*/
	public static void releaseRes(ResultSet rs,Statement pst,Connection con) {
		try {
			if(null!=rs) {
				rs.close();
			}
			if(null!=pst) {
				pst.close();
			}
			if(null!=con) {
				con.close();
			}
		}catch (Exception e) {
			e.printStackTrace();
		}finally {
			rs = null;
			pst = null;
			con = null;
		}
	}

	/**
	* 查询emp表 30号部门员工信息
	* @param args
	* @throws Exception
	*/
	public static void main(String[] args) throws Exception {
		//获取连接
		Connection conn = HiveJdbc.getConn();
		//获取运行资源环境
		Statement pst = conn.createStatement();
		//执行查询
		ResultSet rs = pst.executeQuery("select * from userinfo");
		//遍历结果集
		while(rs.next()) {
			String name = rs.getString(1);
			int age = rs.getInt(2);
			System.out.println(name + "," + age);
		}
		HiveJdbc.releaseRes(rs, pst, conn);
	}
	
}
