package com.hunantv.aws.upload;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.hunantv.aws.core.derby.DerbyUtils;

public class DerbyInit {

	private static Logger LOG = Logger.getLogger(CSVScanner.class);

	/**
	 * 查询表是否存在
	 * 
	 * @return
	 * @throws SQLException
	 */
	public static boolean isTableExist(Connection conn, String tableName)
			throws SQLException {
		boolean hasTable = false;
		ResultSet rs = conn.getMetaData().getTables(null, null, null,
				new String[] { "TABLE" });
		while (rs.next()) {
			String rsTableName = rs.getString("TABLE_NAME");
			if (rsTableName.equalsIgnoreCase(tableName)) {
				hasTable = true;
				break;
			}
		}
		rs.close();
		return hasTable;
	}

	/**
	 * 创建索引表
	 * @param conn
	 * @throws SQLException
	 */
	public static void createIndexTable(Connection conn) throws SQLException {
		LOG.info("create table INDEX_CACHE");
		StringBuffer createSql = new StringBuffer();
		createSql.append("CREATE TABLE INDEX_CACHE(");
		createSql
				.append("ID INT PRIMARY KEY,PID INT,TYPE VARCHAR(1),STATUS VARCHAR(1),");
		createSql
				.append("MD5 VARCHAR(32),START_UPLOAD_TIME BIGINT, UPLOAD_END_TIME BIGINT)");
		Statement st = conn.createStatement();
		st.execute(createSql.toString());
		st.close();
	}

	public static void main(String[] args) {

		if (args.length < 1) {
			LOG.error("input derby database " );
			return;
		}
		LOG.info("init derby tables with args : " + Arrays.toString(args));
		String dbName = args[0];

		String url = "jdbc:derby:" + dbName + ";create=true";

		Connection conn = null;
		try {
			Class.forName(DerbyUtils.DRIVER_NAME);

			conn = DriverManager.getConnection(url);
			if (!isTableExist(conn, "INDEX_CACHE")) {
				createIndexTable(conn);
			}
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
		}
	}
}