package com.hunantv.aws.upload;


public class UploadIndexConstans {

	public final static String INDEX_CACHE_TABLE_NAME = "\"S3_Transfer_Index\"";
	public final static String INDEX_CACHE_ROOT_PID = "-";
	
	/**
	 * 文件类型
	 */
	public final static String FILE_TYPE = "2";
	/**
	 * 文件夹类型，目录
	 */
	public final static String DIRECTORY_TYPE = "1";
	
	/**
	 *  文件夹已完成上传状态
	 */
	public final static String DIR_COMPLETED_STATUS = "2";
	/**
	 * 文件夹未完成上传状态
	 */
	public final static String DIR_NOT_COMPLETED_STATUS = "0";
	
	/**
	 * 文件上传完成
	 */
	public final static String FILE_COMPLETED_STATUS = "2";
	/**
	 * 文件已存入上传队列
	 */
	public final static String FILE_UPLOADING_STATUS = "1";
	/**
	 * 文件未开始上传
	 */
	public final static String FILE_NOT_START_STATUS = "0";
}