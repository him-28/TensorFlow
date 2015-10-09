package com.hunantv.aws.core.s3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

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
import com.amazonaws.util.Md5Utils;
import com.google.common.collect.ImmutableMap;
import com.hunantv.aws.util.CredentialsFileUtil;

public class Awss3 {

	public static Logger LOG = Logger.getLogger(Awss3.class);

	public static final String BUCKET_NAME = "cn-north-region-java";

	private ProfileCredentialsProvider provider;
	private TransferManager tx;

	private AmazonS3 s3;

	private Awss3() {
		init();
	}

//	private static Awss3 instance = new Awss3();

	public static Awss3 newInstance() {
		return new Awss3();
	}

	private void init() {
		provider = CredentialsFileUtil.getDefaultCredentialsProvider();
		AWSCredentials credentials = provider.getCredentials();
		s3 = new AmazonS3Client(credentials);
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
		LOG.info("delete key:" + key);
		List<String> allKey = listAllKey();
		if (!allKey.contains(key)) {
			throw new IllegalArgumentException("Not Found:the key " + key
					+ " did not exists in the bucket.");
		}
		s3.deleteObject(BUCKET_NAME, key);
	}

	public boolean addKey(final String key, String filePath, boolean isOverride) {
		return addKey(key, filePath, isOverride, null, null);
	}

	public boolean addKey(final String key, String filePath,
			boolean isOverride, final Awss3Callback callback,
			final String dataId) {
		final List<Boolean> result = new ArrayList<>(1);
		result.add(false);
		tx = new TransferManager(s3);
		try {
			final long start = System.currentTimeMillis();
			if (!isOverride) {
				List<String> allKey = listAllKey();
				if (allKey.contains(key)) {
					throw new IllegalArgumentException("The key " + key
							+ " already exists in the bucket.");
				}
			}

			LOG.info("upload file:" + filePath + " to :" + key);

			File file = new File(filePath);
			if (file.exists() && file.isFile()) {
				final long fileLength = file.length();
				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentLength(fileLength);

				PutObjectRequest request = new PutObjectRequest(BUCKET_NAME,
						key, file);
				request.setMetadata(metadata);

				final Upload upload = tx.upload(request);

				ProgressListener progressListener = new ProgressListener() {
					public void progressChanged(ProgressEvent progressEvent) {
						if (upload == null)
							return;
						int percentTransferrred = (int) upload.getProgress()
								.getPercentTransferred();
						LOG.debug("upload to :" + key + " "
								+ percentTransferrred + "% completed");
						ProgressEventType type = progressEvent.getEventType();
						switch (type) {
						case TRANSFER_COMPLETED_EVENT:
							percentTransferrred = 100;
							long end = System.currentTimeMillis();
							LOG.info("upload to '" + key
									+ "' completed,total size：" + fileLength
									+ "(bit),time used：" + (end - start)
									+ "(millis)");
							result.set(0, true);

							if (callback != null) {
								callback.notify(ImmutableMap.of("status",
										"success", "id", (Object) dataId));
							}
							break;
						case TRANSFER_FAILED_EVENT:
							try {
								AmazonClientException e = upload
										.waitForException();
								LOG.error("Unable to upload file to Amazon S3: "
										+ e.getMessage());
							} catch (InterruptedException e) {
								LOG.error(e.getMessage(), e);
							}
							if (callback != null) {
								callback.notify(ImmutableMap.of("status",
										"error", "id", (Object) dataId));
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
			} else {
				throw new IllegalArgumentException(
						"Not Found: the file did not exists : " + filePath);
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			if (callback != null) {
				callback.notify(ImmutableMap.of("status", "error", "id",
						(Object) dataId));
			}
		} finally {
			tx.shutdownNow();
		}
		return result.get(0);
	}

	public ObjectMetadata getObjectMetaData(String bucketName, String key) {
		LOG.info("get file meta datas:" + key);
		GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName,
				key);

		S3Object objectPortion = s3.getObject(rangeObjectRequest);
		if (objectPortion == null) {
			return null;
		}

		ObjectMetadata obj = objectPortion.getObjectMetadata();
		return obj;
	}

	public void download(String bucketName, String key, String outputFilePath,
			boolean isOverride) {
		LOG.info("download file:" + key);
		GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName,
				key);

		File of = new File(outputFilePath);
		if (of.exists() && !isOverride) {
			LOG.warn("local file already exits,use -f option to override");
			return;
		}

		S3Object objectPortion = s3.getObject(rangeObjectRequest);
		if (objectPortion == null) {
			return;
		}

		long l = objectPortion.getObjectMetadata().getContentLength();
		LOG.info("total size(bit):" + l);

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
				LOG.info(total + "/" + l + "," + (total * 100 / l)
						+ "% completed");
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
		LOG.info("download complete");
	}

	public static String getMd5String(String filePath)
			throws FileNotFoundException, IOException {
		return bytesToHexString(Md5Utils.computeMD5Hash(new File(filePath)));
	}

	public static String bytesToHexString(byte[] bytes) {
		StringBuffer sb = new StringBuffer(bytes.length * 2);
		String sTemp;
		for (int i = 0; i < bytes.length; i++) {
			sTemp = Integer.toHexString(0xFF & bytes[i]);
			if (sTemp.length() < 2)
				sb.append(0);
			sb.append(sTemp);
		}
		return sb.toString();
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

					Awss3.newInstance().addKey(key, filePath, isOverride);
				} else {
					System.err
							.println("Please add file with command like this : -a (key) (path) [-f]");
				}
				break;
			case "-d": // delete
				if (args.length > 1) {
					String key = args[1];
					Awss3.newInstance().deleteKey(key);
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
					Awss3.newInstance().download(Awss3.BUCKET_NAME, key,
							filePath, isOverride);
				} else {
					System.err
							.println("Please delete file with command like this : -g (key) (path) [-f]");
				}
				break;
			case "-l": // list
				List<String> list = Awss3.newInstance().listAllKey();
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