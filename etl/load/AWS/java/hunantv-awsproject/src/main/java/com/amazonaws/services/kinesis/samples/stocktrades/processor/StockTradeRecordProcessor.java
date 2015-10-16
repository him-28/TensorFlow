/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.samples.stocktrades.processor;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

/**
 * Processes records retrieved from stock trades stream.
 *
 */
public class StockTradeRecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(StockTradeRecordProcessor.class);
    private String kinesisShardId;

    private static final long REPORTING_INTERVAL_MILLIS = 60000L; // 1 minute
    private long nextReportingTimeInMillis;

    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L; // 1 minute
    private long nextCheckpointTimeInMillis;

    private StockStats stockStats = new StockStats();

    private ServerSocket serverSocket = null;
    private Socket socket = null;
    OutputStream os = null;
    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(String shardId) {
        LOG.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
        nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
        nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        
        try {
			System.out.println("serverSocket started1");
			serverSocket = new ServerSocket(9999);
			System.out.println("serverSocket started2");
			socket = serverSocket.accept();
			System.out.println("serverSocket started3");
			os = socket.getOutputStream();
			System.out.println("serverSocket started4");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
    	System.out.println("process record");
        for (Record record : records) {
            processRecord(record);
        }

        if (System.currentTimeMillis() > nextReportingTimeInMillis) {
            reportStats();
            resetStats();
            nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS;
        }

        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    private void reportStats() {
    	System.out.println("****** Shard " + kinesisShardId + " stats for last 1 minute ******\n" +
                stockStats + "\n" +
                "****************************************************************\n");
    }

    private void resetStats() {
    	stockStats = new StockStats();
    }

    
    
    
    private void processRecord(Record record) {

    	System.out.println(new String(record.getData().array()));

    	try {
			os.write(record.getData().array());
			os.write("\n".getBytes());
			os.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
        try {
        	os.close();
        	socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        try {
            checkpointer.checkpoint();
        } catch (ShutdownException se) {
            LOG.info("Caught shutdown exception, skipping checkpoint.", se);
        } catch (ThrottlingException e) {
            LOG.error("Caught throttling exception, skipping checkpoint.", e);
        } catch (InvalidStateException e) {
            LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        }
    }

}
