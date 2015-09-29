package com.hunantv.aws.core.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

public class DBUtils {

	private static Logger LOG = Logger.getLogger(DBUtils.class);

	private static String driver = "org.postgresql.Driver";
	private static String url = "jdbc:postgresql://10.100.5.80:11921/adc";
	private static String username = "postgres";
	private static String password = null;

	// 从配置文件中读取jdbc连接，读取失败使用默认值
	static {
		URL ptUrl = ClassLoader
				.getSystemResource("com/hunantv/aws/config/jdbc.properties");
		Properties prop = new Properties();
		InputStream jdbcFileIS = null;
		if (!ptUrl.toString().contains("!")) { // java class file
			String jdbcPt = ptUrl.getFile();
			try {
				prop.load(new FileInputStream(jdbcPt));
			} catch (IOException e) {
				LOG.error(e.getMessage(), e);
			}

		} else {// jar file
			jdbcFileIS = ClassLoader
					.getSystemResourceAsStream("com/hunantv/aws/config/jdbc.properties");// jar
			try {
				prop.load(jdbcFileIS);
			} catch (IOException e) {
				LOG.error(e.getMessage(), e);
			}
		}
		driver = prop.getProperty("jdbc.driver").trim();
		url = prop.getProperty("jdbc.url").trim();
		username = prop.getProperty("jdbc.username").trim();
		password = prop.getProperty("jdbc.password").trim();
		
		if(jdbcFileIS != null){
			try {
				jdbcFileIS.close();
			} catch (IOException e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}

	/**
	 * 获取新的Connection
	 * 
	 * Connection 默认使用最高级别事务SERIALIZABLE，AutoCommit属性是false
	 * 
	 * •READ UNCOMMITTED：最低级别的隔离，通常又称为dirty
	 * read，它允许一个事务读取还没commit的数据，这样可能会提高性能，但是dirty read可能不是我们想要的。 <br/>
	 * •READ COMMITTED：在一个事务中只允许已经commit的记录可见。如果session中select还在查询中，
	 * 另一session此时insert一条记录，当前事务可以看到修改的记录，从而产生不可重复读取和幻像数据。 <br/>
	 * •REPEATABLE READ：在一个事务开始后，其他session对数据库的修改在本事务中不可见，直到本事务commit或rollback。
	 * 在一个事务中重复select的结果一样，除非本事务中update数据库。 <br/>
	 * •SERIALIZABLE：最高级别的隔离，只允许事务串行执行。为了达到此目的，数据库会锁住每行已经读取的记录，
	 * 其他session不能修改数据直到前一事务结束，事务commit或取消时才释放锁。
	 * 
	 * @param dbName
	 *            数据库名
	 * @return
	 * @throws SQLException
	 */
	public static Connection newConnection() throws SQLException {
		String connUrl = url;
		Connection conn = null;
		try {
			Class.forName(driver).newInstance();
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			LOG.error(e.getMessage(), e);
			return null;
		}
		conn = DriverManager.getConnection(connUrl, username, password);
		conn.setAutoCommit(false);
		conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		return conn;
	}

	public static void execute(String sql) throws SQLException {
		Connection conn = newConnection();
		PreparedStatement pst = conn.prepareStatement(sql);
		LOG.info("excute sql:" + sql);
		pst.execute();
		pst.close();
		conn.commit();
		conn.close();
	}

	public static Map<String, Object> queryFirst(String dbName, String sql,
			String... colNames) throws SQLException {
		LOG.info("query sql:" + sql + ", |" + dbName);
		Connection conn = newConnection();
		PreparedStatement pst = conn.prepareStatement(sql);
		ResultSet rs = pst.executeQuery();
		Map<String, Object> map = new HashMap<>();
		if (rs.next()) { // 只取第一行
			for (String col : colNames) {
				map.put(col, rs.getObject(col));
			}
		}
		rs.close();
		pst.close();
		conn.close();
		return map;
	}

	public static void main(String[] args) throws SQLException {
		Connection conn = newConnection();
		conn.prepareCall("delete  FROM \"S3_Transfer_Index\"  ").execute();
		conn.commit();
	}
}