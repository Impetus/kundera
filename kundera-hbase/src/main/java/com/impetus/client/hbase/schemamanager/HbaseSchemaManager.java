package com.impetus.client.hbase.schemamanager;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.configure.schema.ColumnInfo;
import com.impetus.kundera.configure.schema.EmbeddedColumnInfo;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;

public class HbaseSchemaManager extends AbstractSchemaManager implements
		SchemaManager {
	private static HBaseAdmin admin;
	private static final Logger logger = LoggerFactory
			.getLogger(HbaseSchemaManager.class);

	@Override
	public void exportSchema() {
		super.exportSchema();
	}

	protected void update(List<TableInfo> tableInfos) {
		for (TableInfo tableInfo : tableInfos) {
			HTableDescriptor hTableDescriptor = getTableMetaData(tableInfo);
			try {
				HTableDescriptor descriptor = admin
						.getTableDescriptor(tableInfo.getTableName().getBytes());
				if (!hTableDescriptor.equals(descriptor)) {
					admin.disableTable(tableInfo.getTableName().getBytes());
					HColumnDescriptor[] descriptors = descriptor
							.getColumnFamilies();
					for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas()) {
						boolean found = false;
						HColumnDescriptor columnDescriptor = new HColumnDescriptor(
								columnInfo.getColumnName());
						for (HColumnDescriptor hColumnDescriptor : descriptors) {
							if (hColumnDescriptor.equals(columnDescriptor)) {
								found = true;
								break;
							}
						}
						if (!found) {
							admin.addColumn(tableInfo.getTableName(),
									columnDescriptor);
						}
					}
					for (EmbeddedColumnInfo embeddedColumnInfo : tableInfo
							.getEmbeddedColumnMetadatas()) {
						boolean found = false;
						HColumnDescriptor columnDescriptor = new HColumnDescriptor(
								embeddedColumnInfo.getEmbeddedColumnName());
						for (HColumnDescriptor hColumnDescriptor : descriptors) {
							if (hColumnDescriptor.equals(columnDescriptor)) {
								found = true;
								break;
							}
						}
						if (!found) {
							admin.addColumn(tableInfo.getTableName(),
									columnDescriptor);
						}
					}
				}
			} catch (IOException e) {
				logger.error("check for network connection caused by "
						+ e.getMessage());
			}
		}
	}

	protected void validate(List<TableInfo> tableInfos) {
		for (TableInfo tableInfo : tableInfos) {
			HTableDescriptor hTableDescriptor = getTableMetaData(tableInfo);
			try {
				if (!hTableDescriptor.equals(admin.getTableDescriptor(tableInfo
						.getTableName().getBytes()))) {
					// TODO
					logger.error("");
				}
			} catch (IOException e) {
				logger.error("check for network connection caused by "
						+ e.getMessage());
			}
		}
	}

	protected void create_drop(List<TableInfo> tableInfos) {
		create(tableInfos);

	}

	protected void create(List<TableInfo> tableInfos) {
		for (TableInfo tableInfo : tableInfos) {
			try {
				admin.getTableDescriptor(tableInfo.getTableName().getBytes());
				admin.disableTable(tableInfo.getTableName());
				admin.deleteTable(tableInfo.getTableName());
			} catch (TableNotFoundException e) {

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			HTableDescriptor hTableDescriptor = getTableMetaData(tableInfo);
			try {
				admin.createTable(hTableDescriptor);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void dropSchema() {
		if (operation != null && operation.equalsIgnoreCase("create-drop")) {
			for (TableInfo tableInfo : tableInfos) {
				try {
					admin.disableTable(tableInfo.getTableName());
					admin.deleteTable(tableInfo.getTableName());
				} catch (IOException e) {
					logger.error("check for network connection caused by "
							+ e.getMessage());
				}
			}
		}
	}

	private HTableDescriptor getTableMetaData(TableInfo tableInfo) {
		HTableDescriptor hTableDescriptor = new HTableDescriptor(
				tableInfo.getTableName());
		if (tableInfo.getColumnMetadatas() != null) {
			for (ColumnInfo columnInfo : tableInfo.getColumnMetadatas()) {
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(
						columnInfo.getColumnName());
				hTableDescriptor.addFamily(hColumnDescriptor);
			}
		}
		if (tableInfo.getEmbeddedColumnMetadatas() != null) {
			for (EmbeddedColumnInfo embeddedColumnInfo : tableInfo
					.getEmbeddedColumnMetadatas()) {
				HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(
						embeddedColumnInfo.getEmbeddedColumnName());
				hTableDescriptor.addFamily(hColumnDescriptor);
			}
		}
		return hTableDescriptor;
	}

	protected boolean initiateClient() {
		if (kundera_client.equalsIgnoreCase("Hbase")) {
			Configuration hadoopConf = new Configuration();
			hadoopConf.set("hbase.master", host + ":" + port);
			HBaseConfiguration conf = new HBaseConfiguration(hadoopConf);
			try {
				admin = new HBaseAdmin(conf);
			} catch (MasterNotRunningException e) {
				logger.equals("master not running exception" + e.getMessage());
			} catch (ZooKeeperConnectionException e) {
				logger.equals("zookeeper connection exception" + e.getMessage());
			}
			return true;
		}
		return false;
	}
}
