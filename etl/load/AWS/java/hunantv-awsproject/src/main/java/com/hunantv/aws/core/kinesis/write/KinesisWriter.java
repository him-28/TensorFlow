package com.hunantv.aws.core.kinesis.write;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.samples.stocktrades.utils.ConfigurationUtils;
import com.amazonaws.services.kinesis.samples.stocktrades.utils.CredentialUtils;

public class KinesisWriter {

	private static final Log LOG = LogFactory.getLog(KinesisWriter.class);

	private static void checkUsage(String[] args) {
		if (args.length != 2) {
			System.err.println("Usage: " + KinesisWriter.class.getSimpleName()
					+ " <stream name> <region>");
			System.exit(1);
		}
	}

	/**
	 * Checks if the stream exists and is active
	 * 
	 * @param kinesisClient
	 *            Amazon Kinesis client instance
	 * @param streamName
	 *            Name of stream
	 */
	private static void validateStream(AmazonKinesis kinesisClient,
			String streamName) {
		try {
			DescribeStreamResult result = kinesisClient
					.describeStream(streamName);
			if (!"ACTIVE".equals(result.getStreamDescription()
					.getStreamStatus())) {
				System.err
						.println("Stream "
								+ streamName
								+ " is not active. Please wait a few moments and try again.");
				System.exit(1);
			}
		} catch (ResourceNotFoundException e) {
			System.err.println("Stream " + streamName
					+ " does not exist. Please create it in the console.");
			System.err.println(e);
			System.exit(1);
		} catch (Exception e) {
			System.err.println("Error found while describing the stream "
					+ streamName);
			System.err.println(e);
			System.exit(1);
		}
	}

	/**
	 * Uses the Kinesis client to send a dataLine
	 * 
	 * @param trade
	 *            instance representing the stock trade
	 * @param kinesisClient
	 *            Amazon Kinesis client
	 * @param streamName
	 *            Name of stream
	 */
	private static void sendDataLine(String dataLine,
			AmazonKinesis kinesisClient, String streamName) {
		if (dataLine == null) {
			LOG.warn("Could not get bytes for dataLine");
			return;
		}
		byte[] bytes = dataLine.getBytes();

		LOG.info("Putting trade: " + dataLine);
		PutRecordRequest putRecord = new PutRecordRequest();
		putRecord.setStreamName(streamName);
		putRecord.setData(ByteBuffer.wrap(bytes));
		putRecord.setPartitionKey(dataLine.split("\t")[0]);
		try {
			kinesisClient.putRecord(putRecord);
		} catch (AmazonClientException ex) {
			LOG.warn("Error sending record to Amazon Kinesis.", ex);
		}
	}

	public static void main(String[] args) throws Exception {

		String streamName = "StockTradeStream";
		String regionName = "ap-northeast-1";

		AWSCredentials credentials = CredentialUtils.getCredentialsProvider()
				.getCredentials();

		Region region = RegionUtils.getRegion(regionName);
		AmazonKinesis kinesisClient = new AmazonKinesisClient(credentials,
				ConfigurationUtils.getClientConfigWithUserAgent());
		kinesisClient.setRegion(region);

		validateStream(kinesisClient, streamName);

		while (true) {
			FileReader fr = new FileReader(
					"F:\\git\\amble\\etl\\load\\demand\\4389\\201509\\20150907.17.0.demand.csv");
			BufferedReader br = new BufferedReader(fr);
			String str = null;
			while ((str = br.readLine()) != null) {
				sendDataLine(str, kinesisClient, streamName);
			}
			br.close();
			fr.close();
			Thread.sleep(1000);
		}

		// checkUsage(args);
		//
		// String streamName = args[0];
		// String regionName = args[1];
		// Region region = RegionUtils.getRegion(regionName);
		// if (region == null) {
		// System.err.println(regionName + " is not a valid AWS region.");
		// System.exit(1);
		// }
		//
		// AWSCredentials credentials = CredentialUtils.getCredentialsProvider()
		// .getCredentials();
		//
		// AmazonKinesis kinesisClient = new AmazonKinesisClient(credentials,
		// ConfigurationUtils.getClientConfigWithUserAgent());
		// kinesisClient.setRegion(region);
		//
		// validateStream(kinesisClient, streamName);
		//
		// while (true) {
		// String dataLine = UUID.randomUUID().toString() + "\t"
		// + new Random().nextDouble();
		// sendDataLine(dataLine, kinesisClient, streamName);
		// Thread.sleep(100);
		// }
	}
}