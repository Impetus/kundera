/*
 * Copyright 2010 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.client;

import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.impetus.kundera.CassandraClient;

/**
 * The Class ThriftClient.
 * 
 * @author animesh.kumar
 * @since 0.1
 */
public class ThriftClient implements CassandraClient {

	/** cassandra host. */
	private String contactNode = "localhost";

	/** default port. */
	private int defaultPort = 9160;

	/** The tr. */
	private TTransport tr = null;

	/**
	 * Open up a new connection to the Cassandra Database.
	 * 
	 * @return the Cassandra Client
	 */
	protected Cassandra.Client setupConnection() {
		try {
			tr = new TSocket(contactNode, defaultPort);
			TProtocol proto = new TBinaryProtocol(tr);
			Cassandra.Client client = new Cassandra.Client(proto);
			tr.open();

			return client;
		} catch (TTransportException exception) {
			exception.printStackTrace();
		}

		return null;
	}

	/**
	 * Close the connection to the Cassandra Database.
	 */
	protected void closeConnection() {
		try {
			tr.flush();
			tr.close();
		} catch (TTransportException exception) {
			exception.printStackTrace();
		}
	}

	/*
	 * @see com.impetus.kundera.CassandraClient#delete(java.lang.String,
	 * java.lang.String, java.lang.String)
	 */
	@Override
	public void delete(String keyspace, String columnFamily, String rowId)
			throws Exception {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see com.impetus.kundera.CassandraClient#loadColumns(java.lang.String,
	 * java.lang.String, java.lang.String)
	 */
	@Override
	public List<Column> loadColumns(String keyspace, String columnFamily,
			String rowId) throws Exception {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see com.impetus.kundera.CassandraClient#loadColumns(java.lang.String,
	 * java.lang.String, java.lang.String[])
	 */
	@Override
	public Map<String, List<Column>> loadColumns(String keyspace,
			String columnFamily, String... rowIds) throws Exception {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see
	 * com.impetus.kundera.CassandraClient#loadSuperColumns(java.lang.String,
	 * java.lang.String, java.lang.String, java.lang.String[])
	 */
	@Override
	public List<SuperColumn> loadSuperColumns(String keyspace,
			String columnFamily, String key, String... superColumnNames)
			throws Exception {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see
	 * com.impetus.kundera.CassandraClient#loadSuperColumns(java.lang.String,
	 * java.lang.String, java.lang.String[])
	 */
	@Override
	public Map<String, List<SuperColumn>> loadSuperColumns(String keyspace,
			String columnFamily, String... rowIds) throws Exception {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see com.impetus.kundera.CassandraClient#writeColumns(java.lang.String,
	 * java.lang.String, java.lang.String, org.apache.cassandra.thrift.Column[])
	 */
	@Override
	public void writeColumns(String keyspace, String columnFamily,
			String rowId, Column... columns) throws Exception {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see
	 * com.impetus.kundera.CassandraClient#writeSuperColumns(java.lang.String,
	 * java.lang.String, java.lang.String,
	 * org.apache.cassandra.thrift.SuperColumn[])
	 */
	@Override
	public void writeSuperColumns(String keyspace, String columnFamily,
			String rowId, SuperColumn... superColumns) throws Exception {
		throw new NotImplementedException("TODO");
	}

	/*
	 * @see
	 * com.impetus.kundera.CassandraClient#setContactNodes(java.lang.String[])
	 */
	@Override
	public void setContactNodes(String... contactNodes) {
		this.contactNode = contactNodes[0];
	}

	/* @see com.impetus.kundera.CassandraClient#setDefaultPort(int) */
	@Override
	public void setDefaultPort(int defaultPort) {
		this.defaultPort = defaultPort;
	}

	/* @see com.impetus.kundera.CassandraClient#getCassandraClient() */
	@Override
	public Client getCassandraClient() throws Exception {
		throw new NotImplementedException("TODO");
	}

	/* @see com.impetus.kundera.CassandraClient#shutdown() */
	@Override
	public void shutdown() {

	}

	/* @see com.impetus.kundera.CassandraClient#connect() */
	@Override
	public void connect() {
		throw new NotImplementedException("TODO");
	}
}
