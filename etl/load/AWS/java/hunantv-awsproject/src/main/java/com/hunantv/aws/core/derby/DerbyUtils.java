package com.hunantv.aws.core.derby;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.amazonaws.util.Md5Utils;

public class DerbyUtils {

	private static Logger LOG = Logger.getLogger(DerbyUtils.class);

	public static String DRIVER_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
	private static String url = "jdbc:derby:{dbName};";

	private static Map<String, Connection> connectionMap = new ConcurrentHashMap<>();

	/**
	 * 获取DerbyConnection实例（单例）
	 * 
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
	public static Connection derbyConnectionInstance(String dbName)
			throws SQLException {
		synchronized (connectionMap) {
			if (connectionMap.containsKey(dbName)) {
				return connectionMap.get(dbName);
			} else {
				String connUrl = url.replace("{dbName}", dbName);
				Connection conn = null;
				try {
					Class.forName(DRIVER_NAME);
				} catch (ClassNotFoundException e) {
					LOG.error(e.getMessage(), e);
					return null;
				}
				conn = DriverManager.getConnection(connUrl);
				connectionMap.put(dbName, conn);
				conn.setAutoCommit(false);
				conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
				return conn;
			}
		}
	}

	public static void closeConnectionInstance(String dbName) {
		String url = "jdbc:derby:{dbName};shutdown=true";
		if (dbName == null) {
			dbName = "";
		}
		url = url.replace("{dbName}", dbName);
		synchronized (connectionMap) {
			connectionMap.remove(dbName);
			try {
				if (!derbyConnectionInstance(dbName).isClosed()) {
					DriverManager.getConnection(url);
				}
			} catch (SQLException e) {
				// u can do nothing
			}
		}
	}


	public static void main(String[] args) throws SQLException,
			FileNotFoundException, IOException {

		System.out.println((long) Math.scalb(1, 3));
		System.out.println((long) Math.scalb(1, 31));
		System.out.println((long) Math.scalb(1, 63));
		System.out.println(System.currentTimeMillis());

		System.out.println(Md5Utils.md5AsBase64(
				new File("D://Tools/dotnetfx45_full_x86_x64.exe")).length());
		// Connection conn = derbyConnectionInstance("C:\\Derby\\S3_Index");
		// conn.createStatement().execute("create table testtable(id int primary key, name varchar(20))");
		// conn.createStatement().execute("insert into testtable values(1,'aaa')");
		// ResultSet rs = conn.createStatement().executeQuery(
		// "select id,name from testtable");
		// while (rs.next()) {
		// System.out.println(rs.getInt("id"));
		// System.out.println(rs.getString("name"));
		// }
		// closeConnectionDb("C:\\Derby\\S3_Index");
	}
}
