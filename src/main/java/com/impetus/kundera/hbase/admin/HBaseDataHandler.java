/**
 * 
 */
package com.impetus.kundera.hbase.admin;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.kundera.hbase.client.HBaseData;
import com.impetus.kundera.hbase.client.Reader;
import com.impetus.kundera.hbase.client.Writer;
import com.impetus.kundera.hbase.client.service.HBaseReader;
import com.impetus.kundera.hbase.client.service.HBaseWriter;
import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * @author impetus
 */
public class HBaseDataHandler implements DataHandler {
	
	private HBaseConfiguration conf;
	private HBaseAdmin admin;
	private Reader hbaseReader = new HBaseReader();
	private Writer hbaseWriter  = new HBaseWriter();
    
    
	public HBaseDataHandler(String hostName, String port) {
		try {
			init(hostName, port);
		} catch (MasterNotRunningException e) {
		    throw new RuntimeException(e.getMessage());
		}
	}

	@Override
	public void createTable(final String tableName, final String... colFamily)	throws IOException {
		HTableDescriptor htDescriptor = new HTableDescriptor(tableName);
		for (String columnFamily : colFamily) {
			HColumnDescriptor familyMetadata = new HColumnDescriptor(columnFamily);
			htDescriptor.addFamily(familyMetadata);
		}
		admin.createTable(htDescriptor);
	}
	
	@Override
	public HBaseData populateData(final String tableName, final String  columnFamily, final String[] columnName, final String rowKey) throws IOException {
		return hbaseReader.LoadData(gethTable(tableName), columnFamily, columnName, rowKey);
	}


	@Override
	public void loadData(String tableName, String columnFamily, String rowKey, List<Column> columns, EnhancedEntity e) throws IOException {
		onLoad(tableName, columnFamily);
		hbaseWriter.writeColumns(gethTable(tableName), columnFamily, rowKey, columns, e);
	}

	private void onLoad(String tableName, String columnFamily)	throws MasterNotRunningException, IOException {
		if(!admin.tableExists(Bytes.toBytes(tableName))){
			createTable(tableName, columnFamily);
		}
	}

	/* (non-Javadoc)
	 * @see com.impetus.kundera.hbase.admin.Loader#loadConfiguration(java.lang.String, java.lang.String)
	 */
	private void  loadConfiguration(final String hostName, final String port) throws MasterNotRunningException {
				Configuration hadoopConf = new Configuration();
				hadoopConf.set("hbase.master", hostName+":"+port);
				conf = new  HBaseConfiguration(hadoopConf);
				getHBaseAdmin();
		}
	
	/**
	 * 
	 * @param hostName
	 * @param port
	 * @throws MasterNotRunningException
	 */
	private  void init(final String hostName, final String port) throws MasterNotRunningException{
		if(conf ==null) {
			loadConfiguration(hostName, port);
		}
	}


	/**
     * @throws MasterNotRunningException
     */
	private void getHBaseAdmin() throws MasterNotRunningException{
		admin = new HBaseAdmin(conf);
	}
	
	private HTable gethTable(final String tableName) throws IOException{
		return new HTable(conf, tableName);
	}

	@Override
	public void shutdown() {
		try {
			admin.shutdown();
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

}
