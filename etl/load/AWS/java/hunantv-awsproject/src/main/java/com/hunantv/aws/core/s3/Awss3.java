package com.hunantv.aws.core.s3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.hunantv.aws.util.CredentialsFileUtil;

public class Awss3 {

	public static Logger LOG = LoggerFactory.getLogger(Awss3.class);
	
	public static final String BUCKET_NAME = "cn-north-region-java";

	private ProfileCredentialsProvider provider;
	private TransferManager tx;

	private AmazonS3 s3;

	private Awss3() {
		init();
	}

	private static Awss3 instance = new Awss3();

	public static Awss3 getInstance() {
		return instance;
	}

	private boolean isRunnableJar = false;

	private void init() {
		provider = CredentialsFileUtil.getDefaultCredentialsProvider();
		AWSCredentials credentials = provider.getCredentials();
		s3 = new AmazonS3Client(credentials);
		tx = new TransferManager(s3);
	}

	public List<String> listAllKey() {
		ObjectListing listing = s3.listObjects(BUCKET_NAME);
		List<String> result = new ArrayList<String>();
		if (listing == null) {
			return result;
		}
		List<S3ObjectSummary> objectList = listing.getObjectSummaries();
		if (objectList == null || objectList.size() == 0) {
			return result;
		}
		for (S3ObjectSummary summary : objectList) {
			result.add(summary.getKey());
		}
		return result;
	}

	public void deleteKey(String key) {
		System.out.println("删除：" + key);
		List<String> allKey = listAllKey();
		if (!allKey.contains(key)) {
			throw new IllegalArgumentException("Not Found:the key " + key
					+ " did not exists in the bucket.");
		}
		s3.deleteObject(BUCKET_NAME, key);
	}

	public boolean addKey(String key, String filePath, boolean isOverride) {
		final List<Boolean> result = new ArrayList<>(1);
		result.add(false);
		final long start = System.currentTimeMillis();
		if (!isOverride) {
			List<String> allKey = listAllKey();
			if (allKey.contains(key)) {
				throw new IllegalArgumentException("The key " + key
						+ " already exists in the bucket.");
			}
		}

		System.out.println("开始上传：" + filePath);
		System.out.println("key:" + key);

		File file = new File(filePath);
		if (file.exists() && file.isFile()) {
			final long fileLength = file.length();
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(fileLength);

			PutObjectRequest request = new PutObjectRequest(BUCKET_NAME, key,
					file);
			request.setMetadata(metadata);

			final Upload upload = tx.upload(request);

			ProgressListener progressListener = new ProgressListener() {
				public void progressChanged(ProgressEvent progressEvent) {
					if (upload == null)
						return;
					int percentTransferrred = (int) upload.getProgress()
							.getPercentTransferred();
					System.out.println(percentTransferrred + "% completed");
					ProgressEventType type = progressEvent.getEventType();
					switch (type) {
					case TRANSFER_COMPLETED_EVENT:
						percentTransferrred = 100;
						long end = System.currentTimeMillis();
						System.out.println("文件大小：" + fileLength + "(bt),共耗时："
								+ (end - start) + "(millis)");
						result.set(0, true);
						break;
					case TRANSFER_FAILED_EVENT:
						try {
							AmazonClientException e = upload.waitForException();
							System.out
									.println("Unable to upload file to Amazon S3: "
											+ e.getMessage());
						} catch (InterruptedException e) {
						}
						break;
					default:
						break;
					}
				}
			};
			upload.addProgressListener(progressListener);
			try {
				upload.waitForCompletion();
			} catch (AmazonClientException | InterruptedException e) {
				e.printStackTrace();
			}
			if (isRunnableJar) {
				tx.shutdownNow();
			}
		} else {
			throw new IllegalArgumentException(
					"Not Fount: the file did not exists.");
		}

		return result.get(0);
	}

	public void download(String bucketName, String key, String outputFilePath,
			boolean isOverride) {
		System.out.println("download:" + key);
		GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName,
				key);

		File of = new File(outputFilePath);
		if (of.exists() && !isOverride) {
			System.err.println("file already exits,use -f option to override");
			return;
		}

		S3Object objectPortion = s3.getObject(rangeObjectRequest);
		if (objectPortion == null) {
			return;
		}

		long l = objectPortion.getObjectMetadata().getContentLength();
		System.out.println("文件大小：" + l);

		InputStream objectData = objectPortion.getObjectContent();
		if (objectData == null) {
			return;
		}
		FileOutputStream fos = null;

		byte[] b = new byte[1024];
		long total = 0L;

		try {
			fos = new FileOutputStream(outputFilePath);
			int len = -1;
			while ((len = objectData.read(b)) != -1) {
				total = total + len;
				System.out.println("已下载：" + total + "/" + l + ","
						+ (total * 100 / l) + '%');
				fos.write(b);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				objectData.close();
				if (fos != null) {
					fos.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("download complete");
	}

	public static void main(String[] args) throws IOException {
		if (args != null && args.length > 0) {
			String oper = args[0];
			switch (oper) {
			case "-a": // add
				if (args.length > 2) {
					String key = args[1];
					String filePath = args[2];
					boolean isOverride = false;
					if (args.length > 3) {
						isOverride = "-f".equalsIgnoreCase(args[3])
								|| "-FORCE".equalsIgnoreCase(args[3]);
					}

					Awss3.getInstance().addKey(key, filePath, isOverride);
				} else {
					System.err
							.println("Please add file with command like this : -a (key) (path) [-f]");
				}
				break;
			case "-d": // delete
				if (args.length > 1) {
					String key = args[1];
					Awss3.getInstance().deleteKey(key);
				} else {
					System.err
							.println("Please delete file with command like this : -d (key) (path)");
				}
				break;
			case "-g": // get
				if (args.length > 2) {
					String key = args[1];
					String filePath = args[2];
					boolean isOverride = args.length > 3
							&& "-f".equals(args[3]);
					Awss3.getInstance().download(Awss3.BUCKET_NAME, key,
							filePath, isOverride);
				} else {
					System.err
							.println("Please delete file with command like this : -g (key) (path) [-f]");
				}
				break;
			case "-l": // list
				List<String> list = Awss3.getInstance().listAllKey();
				for (String s : list) {
					System.out.println(s);
				}
				break;
			case "-h": // help
			default:
				System.out.println("-a (key) (path) [-f]  :  添加文件");
				System.out.println("-d (key)  : 删除文件");
				System.out.println("-g (key) (path) [-f] : 下载文件");
				System.out.println("-l  : 文件列表");
				System.out.println("-h : 显示此帮助");
				break;
			}

		} else {
			System.err.println("-a (key) (path)  [-f]  :  添加文件");
			System.err.println("-d (key)  : 删除文件");
			System.err.println("-g (key) (path) [-f] : 下载文件");
			System.err.println("-l  : 文件列表");
			System.err.println("-h : 显示此帮助");
		}
	}
}