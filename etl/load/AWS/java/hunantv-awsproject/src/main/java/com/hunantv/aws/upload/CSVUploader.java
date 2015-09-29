package com.hunantv.aws.upload;

import java.nio.channels.FileLock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.hunantv.aws.core.s3.Awss3;
import com.hunantv.aws.core.s3.Awss3Callback;

public class CSVUploader {

	private static Logger LOG = Logger.getLogger(CSVScanner.class);

	public static final Map<String, FileLock> UPLOAD_FILE_LOCKS = Collections
			.synchronizedMap(new HashMap<String, FileLock>());

	private ExecutorService uploadPool = null;

	private CSVUploader() {

	}

	private static CSVUploader uploader = new CSVUploader();

	private volatile static boolean hasInit = false;

	public void init(int nThreads) {
		if (!hasInit) {
			hasInit = true;
			this.uploadPool = Executors.newFixedThreadPool(nThreads);
		}
	}

	public void stop() {
		LOG.info("stop to receive upload request");
		try {
			this.uploadPool.shutdown();
			this.uploadPool.awaitTermination(10L, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			LOG.error(e.getLocalizedMessage(), e);
		}
	}

	public static CSVUploader getInstance() {
		if (!hasInit) {
			throw new RuntimeException("please init this instance first");
		}
		return uploader;
	}

	public static CSVUploader initInstance(int nThreads) {
		uploader.init(nThreads);
		return uploader;
	}

	/**
	 * 添加文件到队列
	 * 
	 * @return
	 */
	public boolean add2Queue(Map<String, Object> fileInfo, CSVScanner scanner) {
		if (!uploadPool.isShutdown()) {
			String fileId = (String) fileInfo.get("id");
			try {
				uploadPool.execute(new UploadThread(fileInfo, scanner));
				scanner.uploadingFile(fileId);
				LOG.info("queue file:" + fileInfo.get("root_path") + " --> "
						+ fileInfo.get("path_name") + ",|"
						+ fileInfo.get("index_name"));
			} catch (SecurityException e) {
				LOG.error("没有权限读写或锁定文件，已跳过：" + fileInfo.get("root_path"), e);
			}

		} else {
			LOG.warn("queue file timeout, please continue in next turn:"
					+ fileInfo.get("root_path") + " --> "
					+ fileInfo.get("path_name") + ",|"
					+ fileInfo.get("index_name"));
		}
		return false;
	}

	private class UploadThread extends Thread {
		private Map<String, Object> fileInfo = null;
		private CSVScanner scanner = null;

		public UploadThread(Map<String, Object> fileInfo, CSVScanner scanner) {
			this.fileInfo = fileInfo;
			this.scanner = scanner;
		}

		private Awss3Callback getAwss3Callback(final String id, final String key) {
			return new Awss3Callback() {
				@Override
				public void notify(Map<String, Object> infomation) {
					if ("success".equals(infomation.get("status"))) { // 上传成功
						String s3Md5 = Awss3.newInstance()
								.getObjectMetaData(Awss3.BUCKET_NAME, key)
								.getETag();
						LOG.info("upload to “" + key
								+ "” success, the local md5 code is: "
								+ fileInfo.get("file_md5")
								+ ", the s3 md5 code is :" + s3Md5);
						if (fileInfo.get("file_md5").equals(s3Md5)) {
							// MD5一致，上传成功
							scanner.finishFileUpload(id);
						} else {// 上传失败
							scanner.errorFileUpload(id); // 回滚数据库状态
						}
					} else { // 上传失败
						LOG.info("upload to “" + key
								+ "” completed with error status");
						scanner.errorFileUpload(id); // 回滚数据库状态
					}
				}
			};
		}

		@Override
		public void run() {
			String pathName = (String) fileInfo.get("path_name");
			final String key = pathName.replace("\\", "/");
			String filePath = (String) fileInfo.get("root_path");
			final String id = (String) fileInfo.get("id");
			
			Awss3.newInstance().addKey(key, filePath, true,
					getAwss3Callback(id, key), id);
		}
	}
	

	public static void main(String[] args) throws Exception {
		final CSVUploader c = new CSVUploader();
		final ExecutorService a = c.a();

		System.out.println("shutdonw");
		a.shutdown();
		a.awaitTermination(10, TimeUnit.SECONDS);
	}

	public ExecutorService a() {
		ExecutorService a = Executors.newFixedThreadPool(2);
		a.submit(new Runnable() {
			@Override
			public void run() {
				System.out.println("a");
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("b");
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("c");
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		return a;
	}
}