package com.hunantv.aws.upload;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.hunantv.aws.core.derby.DerbyUtils;

public class CSVScanner {

	private static Logger log = Logger.getLogger(CSVScanner.class);

	// 本地数据存放根目录
	private String rootPath = null;
	private String dbName = null;

	public CSVScanner(String rootPath, String dbName) {
		this.rootPath = rootPath;
		this.dbName = dbName;
	}

	public void scan() {
		File rootFile = new File(rootPath);
		File[] children = rootFile.listFiles();
		for (File child : children) {
			if (child.isDirectory()) { // 目录
				String name = child.getName();
			}
		}
	}

	// private boolean isDirectoryInited(){
	// Connection conn = DerbyUtils.derbyConnectionInstance(dbName);
	// DerbyUtils.initIndexTable(conn);
	//
	// String sql = "select"
	// }

	/**
	 * 查询一个目录上传是否完成
	 * 
	 * @return
	 */
	// private boolean isDirectoryUploadCompleted(){
	//
	// }

	/**
	 * args:
	 * 
	 * /data/ad2/demand /root/etl-aws/derby/s3_demand_index
	 * 
	 * 或者：
	 * 
	 * /data/ad2/supply /root/etl-aws/derby/s3_supply_index
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			log.error("can not started scanner with args : "
					+ Arrays.toString(args));
			return;
		}
		log.info("scanner started with args : " + Arrays.toString(args));
		String dataRootPath = args[0];
		String dbName = args[1];

		Connection conn = null;
		try {
			conn = DerbyUtils.derbyConnectionInstance(dbName);
			// 最高级别事务开启
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		} catch (SQLException e) {
			log.error(e.getMessage(), e);
		}
		if (conn != null) {
			try {
				new CSVScanner(dataRootPath, dbName).scan();
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}
		}
	}
}