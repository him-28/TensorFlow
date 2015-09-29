package com.hunantv.aws.upload;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.hunantv.aws.core.db.DBUtils;
import com.hunantv.aws.core.s3.Awss3;

public class CSVScanner {

	private static Logger log = Logger.getLogger(CSVScanner.class);

	// 本地数据存放根目录
	private String rootPath = null;
	// 上传数据分组名称
	private String indexName = null;
	// 数据最上层目录
	private String grandFolder = null;
	/**
	 * 默认上传开启线程数
	 */
	public static final int DEFAULT_THREAD_NUMBER = 3;
	// 上传开启线程数
	private int threadNumber = DEFAULT_THREAD_NUMBER;
	/**
	 * 默认上传超时时间（秒）
	 */
	public static final long DEFAULT_THREAD_TIMEOUT = 10L;
	// 上传超时，秒，默认为10分钟
	private long timeout = DEFAULT_THREAD_TIMEOUT;

	/**
	 * 
	 * @param rootPath
	 *            本地数据存放根目录
	 * @param indexName
	 *            上传数据分组名称
	 * @param threadNumber
	 *            上传开启线程数
	 */
	public CSVScanner(String rootPath, String indexName, int threadNumber,
			long timeout) {
		rootPath = rootPath.replace("/", File.separator).replace("\\",
				File.separator);
		if (!rootPath.endsWith(File.separator)) {
			rootPath = rootPath + File.separator;
		}
		String regx = "\\".equals(File.separator) ? "\\\\" : File.separator;
		String[] tmpPathArr = rootPath.split(regx);
		this.grandFolder = tmpPathArr[tmpPathArr.length - 1];
		this.rootPath = rootPath;
		this.indexName = indexName;
		this.threadNumber = threadNumber > 0 ? threadNumber
				: DEFAULT_THREAD_NUMBER;
		this.timeout = timeout > 0 ? timeout : DEFAULT_THREAD_TIMEOUT;
	}

	public void scan() {
		scan(UploadIndexConstans.INDEX_CACHE_ROOT_PID, this.rootPath);
		CSVUploader.getInstance().stop();// 执行完毕后关闭
	}

	private void scan(String pid, String filePath) {
		if (filePath == null) {
			filePath = this.rootPath;
		}
		CSVUploader uploader = CSVUploader.initInstance(threadNumber, timeout);

		File rootFile = new File(filePath);
		File[] children = rootFile.listFiles();
		for (File child : children) {
			String s3PathName = getS3Path(child.getAbsolutePath());
			if (child.isDirectory()) { // 目录
				// TODO////////////////////// 事务1--------------------------Start
				Map<String, Object> dirInfo = getDirectoryInfo(s3PathName, pid);
				if (dirInfo.isEmpty()) { // 目录在数据库中不存在
					String id = initDirectory(pid, s3PathName, filePath);
					// TODO//////////////////////
					// 事务1--------------------------End
					scan(id, child.getPath());
				} else {
					String status = (String) dirInfo.get("upload_status");
					if (UploadIndexConstans.DIR_COMPLETED_STATUS.equals(status)) { // 文件夹下所有文件已经传输完成且不会再有文件写入
						continue; // do nothing
					} else if (UploadIndexConstans.DIR_NOT_COMPLETED_STATUS
							.equals(status)) { // 文件夹下文件正在传输、传输未完成或传输完成但还有文件写入
						scan((String) dirInfo.get("id"), child.getPath());
					}
				}
			} else { // 文件
				// TODO////////////////////// 事务2--------------------------Start
				Map<String, Object> fileInfo = getFileInfo(s3PathName, pid);
				if (fileInfo.isEmpty()) { // 文件在数据库中不存在
					fileInfo = initFile(pid, s3PathName, child.getPath());
					// TODO//////////////////////
					// 事务2--------------------------End
					uploader.add2Queue(fileInfo, this);
				} else {
					String status = (String) fileInfo.get("upload_status");
					if (UploadIndexConstans.FILE_COMPLETED_STATUS
							.equals(status)) { // 文件已经传输完成
						continue; // do nothing
					} else if (UploadIndexConstans.FILE_UPLOADING_STATUS
							.equals(status)) { // 文件正在传输
						// TODO CHECK STATUS 检查是否真的在上传队列中
						continue; // do nothing
					} else if (UploadIndexConstans.FILE_NOT_START_STATUS
							.equals(status)) { // 文件未传输
						uploader.add2Queue(fileInfo, this);
					}
				}
			}
		}
	}

	/**
	 * 初始化文件夹在数据库的记录信息
	 * 
	 * @param pid
	 * @param s3PathName
	 * @param localPath
	 * @return
	 */
	public String initDirectory(String pid, String s3PathName, String localPath) {
		log.info("init directory : " + s3PathName);
		String id = UUID.randomUUID().toString(); //
		String sql = "INSERT INTO "
				+ UploadIndexConstans.INDEX_CACHE_TABLE_NAME
				+ "(id,pid,path_name,root_path,upload_status,path_type,file_md5,upload_start_time,upload_end_time,queue_time,index_name) VALUES ('"
				+ id + "','" + pid + "','" + s3PathName + "','" + localPath
				+ "','" + UploadIndexConstans.DIR_NOT_COMPLETED_STATUS + "','"
				+ UploadIndexConstans.DIRECTORY_TYPE
				+ "',NULL,NULL,NULL,NULL,'" + indexName + "')";
		try {
			DBUtils.execute(sql);
			return id;
		} catch (SQLException e) {
			log.error(e.getLocalizedMessage(), e);
		}
		return id;
	}

	/**
	 * 初始化文件在数据库中的记录信息
	 * 
	 * @param pid
	 * @param s3PathName
	 * @param filePath
	 * @return
	 */
	public Map<String, Object> initFile(String pid, String s3PathName,
			String filePath) {
		Map<String, Object> map = new HashMap<String, Object>();
		log.info("init file : " + filePath);

		String id = UUID.randomUUID().toString(); //

		String md5 = null;
		try {
			md5 = Awss3.getMd5String(filePath);
		} catch (FileNotFoundException e) {
			log.error(e.getMessage(), e);
			throw new RuntimeException("找不到文件：" + filePath);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			throw new RuntimeException("读取文件出错：" + filePath);
		}

		map.put("pid", pid);
		map.put("path_name", s3PathName);
		map.put("root_path", filePath);
		map.put("upload_status", UploadIndexConstans.FILE_NOT_START_STATUS);
		map.put("path_type", UploadIndexConstans.FILE_TYPE);
		map.put("id", id);
		map.put("file_md5", md5);
		map.put("index_name", indexName);

		String sql = "INSERT INTO "
				+ UploadIndexConstans.INDEX_CACHE_TABLE_NAME
				+ "(id,pid,path_name,root_path,upload_status,path_type,file_md5,upload_start_time,upload_end_time,queue_time,index_name) VALUES ('"
				+ id + "','" + pid + "','" + s3PathName + "','" + filePath
				+ "','" + UploadIndexConstans.FILE_NOT_START_STATUS + "','"
				+ UploadIndexConstans.FILE_TYPE + "','" + md5
				+ "',NULL,NULL,NULL,'" + indexName + "')";
		try {
			DBUtils.execute(sql);
		} catch (SQLException e) {
			log.error(e.getLocalizedMessage(), e);
			throw new RuntimeException(
					"exception while create file record,filename:" + filePath
							+ ", e:" + e.getMessage());
		}
		return map;
	}

	/**
	 * 标记文件完成上传
	 * 
	 * @param fileId
	 *            文件ID
	 */
	public void finishFileUpload(String fileId) {
		log.info("file upload completed:" + fileId);
		long now = System.currentTimeMillis();
		String sql = "UPDATE " + UploadIndexConstans.INDEX_CACHE_TABLE_NAME
				+ " SET upload_end_time="+now+", upload_status='" + UploadIndexConstans.FILE_COMPLETED_STATUS + "' where id='"
				+ fileId + "' ";
		try {
			DBUtils.execute(sql);
		} catch (SQLException e) {
			log.error(e.getMessage(), e);
		}
	}

	public void uploadingFile(String fileId) {
		log.info("file uploading:" + fileId);
		uploadFileStatus(fileId, UploadIndexConstans.FILE_UPLOADING_STATUS);
	}

	/**
	 * 标记文件上传失败
	 * 
	 * @param id
	 */
	public void errorFileUpload(String fileId) {
		log.info("file upload failed:" + fileId);
		uploadFileStatus(fileId, UploadIndexConstans.FILE_NOT_START_STATUS);
	}

	private void uploadFileStatus(String fileId, String fileUploadStatus) {
		String sql = "UPDATE " + UploadIndexConstans.INDEX_CACHE_TABLE_NAME
				+ " SET upload_status='" + fileUploadStatus + "' where id='"
				+ fileId + "' ";
		try {
			DBUtils.execute(sql);
		} catch (SQLException e) {
			log.error(e.getMessage(), e);
		}
	}

	private String getS3Path(String filePath) {
		int i = filePath.indexOf(File.separator + grandFolder)
				+ grandFolder.length() + 1;
		String result = indexName + filePath.substring(i);
		return result.replace("\\", "/");
	}

	private Map<String, Object> getDirectoryInfo(String dirName, String pid) {
		String sql = "SELECT * FROM "
				+ UploadIndexConstans.INDEX_CACHE_TABLE_NAME
				+ " WHERE index_name='" + indexName + "' AND path_type = '"
				+ UploadIndexConstans.DIRECTORY_TYPE + "' AND path_name='"
				+ dirName + "' AND pid='" + pid + "'";
		try {
			Map<String, Object> rs = DBUtils.queryFirst(indexName, sql,
					"path_name", "upload_status", "id", "pid");
			return rs;
		} catch (SQLException e) {
			log.error(e.getMessage(), e);
			throw new RuntimeException("数据库查询异常");
		}
	}

	private Map<String, Object> getFileInfo(String s3PathName, String pid) {
		String sql = "SELECT * FROM "
				+ UploadIndexConstans.INDEX_CACHE_TABLE_NAME
				+ " WHERE index_name='" + indexName + "' AND path_type = '"
				+ UploadIndexConstans.FILE_TYPE + "' AND path_name='"
				+ s3PathName + "' AND pid='" + pid + "'";
		try {
			Map<String, Object> rs = DBUtils.queryFirst(indexName, sql, "id",
					"pid", "index_name", "path_type", "path_name", "root_path",
					"upload_status", "file_md5", "queue_time",
					"upload_start_time", "upload_end_time");
			return rs;
		} catch (SQLException e) {
			log.error(e.getMessage(), e);
			throw new RuntimeException("数据库查询异常");
		}
	}

	/**
	 * args:
	 * 
	 * /data/ad2/demand demand
	 * (扫描的本地目录)(上传到S3的根目录)[最大同时上传文件数][上传超时时间（秒、超时后不再执行队列里未上传的任务）]
	 * 
	 * 或者：
	 * 
	 * /data/ad2/supply supply
	 * 
	 * 
	 * TEST: F:/data/ad2/demand demand
	 * 
	 * @param args
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		if (args.length < 2) {
			log.error("can not started scanner with args : "
					+ Arrays.toString(args));
			return;
		}
		log.info("scanner started with args : " + Arrays.toString(args));
		String dataRootPath = args[0];
		String indexName = args[1];

		int threadNumber = -1; // keep default
		if (args.length > 2) {
			if (!Pattern.matches("\\d", args[2])) {
				log.error("the third arg should be a number");
				return;
			} else {
				threadNumber = Integer.parseInt(args[2]);
			}
		}

		long timeout = -1;
		if (args.length > 3) {
			if (!Pattern.matches("\\d", args[3])) {
				log.error("the 4th arg should be a number");
				return;
			} else {
				timeout = Long.parseLong(args[3]);
			}
		}
		dataRootPath = dataRootPath.replace("\\", File.separator).replace("/",
				File.separator);
		try {
			CSVScanner scanner = new CSVScanner(dataRootPath, indexName,
					threadNumber, timeout);
			scanner.scan();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}
}