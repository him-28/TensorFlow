package com.hunantv.aws.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;

public class CredentialsFileUtil {

	private static ProfileCredentialsProvider provider = null;

	private static String credentialsProfileName = "default";

	public static ProfileCredentialsProvider getDefaultCredentialsProvider() {
		synchronized (provider) {
			if (provider == null) {
				String credentialsProfilePath = null;
				try {
					URL url = ClassLoader
							.getSystemResource("com/hunantv/aws/config/credentials");
					if (!url.toString().contains("!")) { // java class file
						credentialsProfilePath = url.getFile();
						provider = new ProfileCredentialsProvider(
								credentialsProfilePath, credentialsProfileName);
					} else {// jar file
						InputStream credentialsProfileIS = ClassLoader
								.getSystemResourceAsStream("com/hunantv/aws/config/credentials");// jar
						credentialsProfilePath = System
								.getProperty("java.io.tmpdir")
								+ File.separator
								+ "awss3.tmp";
						File tmpFile = new File(credentialsProfilePath);
						if (tmpFile.exists()) {
							tmpFile.delete();
						}
						FileOutputStream fos = new FileOutputStream(tmpFile);
						byte[] b = new byte[1024];
						while (credentialsProfileIS.read(b) > -1) {
							fos.write(b);
						}
						fos.flush();
						fos.close();
						credentialsProfileIS.close();
						ProfilesConfigFile profilesConfigFile = new ProfilesConfigFile(
								tmpFile);
						provider = new ProfileCredentialsProvider(
								profilesConfigFile, credentialsProfileName);
						tmpFile.delete();
					}
				} catch (Exception e) {
					throw new AmazonClientException(
							"Cannot load the credentials from the credential profiles file. "
									+ "Please make sure that your credentials file is at the correct "
									+ "location (" + credentialsProfilePath
									+ "), and is in valid format.", e);
				}
			}
			return provider;
		}
	}
}