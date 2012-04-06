package com.impetus.client.cassandra.schemamanager;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.configure.schema.ColumnInfo;
import com.impetus.kundera.configure.schema.TableInfo;
import com.impetus.kundera.configure.schema.api.AbstractSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;

public class CassandraSchemaManager extends AbstractSchemaManager implements
		SchemaManager {
	private Cassandra.Client Cassandra_client = null;
	private static final Logger logger = LoggerFactory
			.getLogger(CassandraSchemaManager.class);

	@Override
	public void exportSchema() {
		super.exportSchema();

	}

	protected void create_drop(List<TableInfo> tableInfos) {
		create(tableInfos);

	}

	protected void create(List<TableInfo> tableInfos) {

		try {
			KsDef ksDef = Cassandra_client.describe_keyspace(databaseName);
			addTablesToKeyspace(tableInfos, ksDef);
		} catch (NotFoundException e1) {
			createKeyspaceAndTables(tableInfos);
		} catch (InvalidRequestException e1) {
			logger.error("keyspace " + databaseName + " does not exist :"
					+ e1.getMessage());
			throw new KunderaException(e1);
		} catch (TException e1) {
			logger.error("keyspace " + databaseName + " does not exist :"
					+ e1.getMessage());
			throw new KunderaException(e1);
		} catch (SchemaDisagreementException e) {
			logger.error("keyspace " + databaseName + " does not exist :"
					+ e.getMessage());
			throw new KunderaException(e);
		}
	}

	protected void update(List<TableInfo> tableInfos) {

		try {
			KsDef ksDef = Cassandra_client.describe_keyspace(databaseName);
			addTablesToKeyspace(tableInfos, ksDef);
		} catch (NotFoundException e) {
			createKeyspaceAndTables(tableInfos);
		} catch (InvalidRequestException e) {
			logger.error("keyspace " + databaseName + " does not exist :"
					+ e.getMessage());
			throw new KunderaException(e);
		} catch (TException e) {
			logger.error("keyspace " + databaseName + " does not exist :"
					+ e.getMessage());
			throw new KunderaException(e);
		} catch (SchemaDisagreementException e) {
			logger.error("keyspace " + databaseName + " does not exist :"
					+ e.getMessage());
			throw new KunderaException(e);
		}
	}

	protected void validate(List<TableInfo> tableInfos) {
		try {
			KsDef ksDef = Cassandra_client.describe_keyspace(databaseName);
			checkForTables(tableInfos, ksDef);
		} catch (NotFoundException e) {
			logger.error("keyspace " + databaseName + " does not exist :"
					+ e.getMessage());
			throw new KunderaException(e);
		} catch (InvalidRequestException e) {
			logger.error("keyspace " + databaseName + " does not exist :"
					+ e.getMessage());
			throw new KunderaException(e);
		} catch (TException e) {
			logger.error("keyspace " + databaseName + " does not exist :"
					+ e.getMessage());
			throw new KunderaException(e);
		}
	}

	private void checkForTables(List<TableInfo> tableInfos, KsDef ksDef) {
		try {
			Cassandra_client.set_keyspace(ksDef.getName());
		} catch (InvalidRequestException e) {
			logger.error("keyspace " + databaseName + " does not exist :"
					+ e.getMessage());
			throw new KunderaException(e);
		} catch (TException e) {
			logger.error("keyspace " + databaseName + " does not exist :"
					+ e.getMessage());
			throw new KunderaException(e);
		}
		for (TableInfo tableInfo : tableInfos) {
			for (CfDef cfDef : ksDef.getCf_defs()) {
				if (cfDef.getName().equalsIgnoreCase(tableInfo.getTableName())) {
					if (cfDef.getColumn_type().equals(tableInfo.getType())) {

					} else {
						logger.error("column family "
								+ tableInfo.getTableName()
								+ " does not exist in keyspace " + databaseName
								+ "");
						break;
					}
				} else {
					logger.error("column family " + tableInfo.getTableName()
							+ " does not exist in keyspace " + databaseName
							+ "");
					break;
				}
			}
		}
	}

	private void addTablesToKeyspace(List<TableInfo> tableInfos, KsDef ksDef)
			throws InvalidRequestException, TException,
			SchemaDisagreementException {

		Cassandra_client.set_keyspace(databaseName);
		for (TableInfo tableInfo : tableInfos) {
			boolean found = false;
			for (CfDef cfDef : ksDef.getCf_defs()) {
				if (cfDef.getName().equalsIgnoreCase(tableInfo.getTableName())) {
					found = true;
					Cassandra_client.system_drop_column_family(tableInfo
							.getTableName());
					Cassandra_client
							.system_add_column_family(getTableMetadata(tableInfo));
					break;
				}
			}
			if (!found) {
				Cassandra_client
						.system_add_column_family(getTableMetadata(tableInfo));
			}
		}
	}

	private void createKeyspaceAndTables(List<TableInfo> tableInfos) {
		KsDef ksDef = new KsDef(databaseName,
				"org.apache.cassandra.locator.SimpleStrategy", null);
		ksDef.setReplication_factor(1);
		List<CfDef> cfDefs = new ArrayList<CfDef>();
		for (TableInfo tableInfo : tableInfos) {
			cfDefs.add(getTableMetadata(tableInfo));
		}
		ksDef.setCf_defs(cfDefs);
		try {
			createKeyspace(ksDef);
		} catch (InvalidRequestException e) {
			logger.error("Error during creating cassandra schema, Caused by:"
					+ e.getMessage());
			throw new KunderaException(e);
		} catch (SchemaDisagreementException e) {
			logger.error("Error during creating cassandra schema, Caused by:"
					+ e.getMessage());
			throw new KunderaException(e);
		} catch (TException e) {
			logger.error("Error during creating cassandra schema, Caused by:"
					+ e.getMessage());
			throw new KunderaException(e);
		}
	}

	private void createKeyspace(KsDef ksDef) throws InvalidRequestException,
			SchemaDisagreementException, TException {
		Cassandra_client.system_add_keyspace(ksDef);
	}

	private CfDef getTableMetadata(TableInfo tableInfo) {
		CfDef cfDef = new CfDef();
		cfDef.setKeyspace(databaseName);
		cfDef.setName(tableInfo.getTableName());
		cfDef.setKey_validation_class(CassandraValidationClassMapper
				.getValidationClass(tableInfo.getTableIdType()));
		if (tableInfo.getType().equals("Standard")) {
			cfDef.setColumn_type(tableInfo.getType());
			List<ColumnDef> columnDefs = new ArrayList<ColumnDef>();
			List<ColumnInfo> columnInfos = tableInfo.getColumnMetadatas();
			for (ColumnInfo columnInfo : columnInfos) {
				ColumnDef columnDef = new ColumnDef();
				if (useSecondryIndex && columnInfo.isIndexable()) {
					columnDef.setIndex_type(IndexType.KEYS);
				}
				columnDef.setName(columnInfo.getColumnName().getBytes());
				columnDef.setValidation_class(CassandraValidationClassMapper
						.getValidationClass(columnInfo.getType()));
				columnDefs.add(columnDef);
			}
			cfDef.setColumn_metadata(columnDefs);
		} else {
			cfDef.setColumn_type("Super");
		}
		return cfDef;
	}

	public void dropSchema() {
		if (operation != null && operation.equalsIgnoreCase("create-drop")) {
			try {
				Cassandra_client.set_keyspace(databaseName);
				for (TableInfo tableInfo : tableInfos) {
					Cassandra_client.system_drop_column_family(tableInfo
							.getTableName());
				}
			} catch (InvalidRequestException e) {
				logger.error("keyspace " + databaseName + " does not exist :"
						+ e.getMessage());
				throw new KunderaException(e);
			} catch (TException e) {
				logger.error("keyspace " + databaseName + " does not exist :"
						+ e.getMessage());
				throw new KunderaException(e);
			} catch (SchemaDisagreementException e) {
				logger.error("keyspace " + databaseName + " does not exist :"
						+ e.getMessage());
				throw new KunderaException(e);
			}
		}
	}

	protected boolean initiateClient() {
		if (kundera_client.equalsIgnoreCase("pelops")
				&& Cassandra_client == null) {
			TSocket socket = new TSocket(host, Integer.parseInt(port));
			TTransport transport = new TFramedTransport(socket);
			TProtocol protocol = new TBinaryProtocol(transport);
			Cassandra_client = new Cassandra.Client(protocol);
			try {
				if (!socket.isOpen()) {
					socket.open();
				}
			} catch (TTransportException e) {
				logger.error("Error while opening socket , Caused by:"
						+ e.getMessage());
				throw new KunderaException(e);
			}
			return true;
		}
		return false;
	}
}