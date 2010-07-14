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

    /** keyspace. */
    private String keySpace;

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

    @Override
    public void delete(String columnFamily, String rowId) throws Exception {
    	throw new NotImplementedException("TODO");
    }

    @Override
    public List<Column> loadColumns(String columnFamily, String rowId) throws Exception {
    	throw new NotImplementedException("TODO");
    }

    @Override
    public Map<String, List<Column>> loadColumns(String columnFamily, String... rowIds) throws Exception {
    	throw new NotImplementedException("TODO");
    }

    @Override
    public List<SuperColumn> loadSuperColumns(String columnFamily, String key, String... superColumnNames) throws Exception {
    	throw new NotImplementedException("TODO");
    }

    @Override
    public Map<String, List<SuperColumn>> loadSuperColumns(String columnFamily, String... rowIds) throws Exception {
    	throw new NotImplementedException("TODO");
    }

    @Override
    public void writeColumns(String columnFamily, String rowId, Column... columns) throws Exception {
    	throw new NotImplementedException("TODO");
    }

    @Override
    public void writeSuperColumns(String columnFamily, String rowId, SuperColumn... superColumns) throws Exception {
    	throw new NotImplementedException("TODO");
    }

    @Override
    public void setContactNodes(String... contactNodes) {
        this.contactNode = contactNodes[0];
    }

    @Override
    public void setDefaultPort(int defaultPort) {
        this.defaultPort = defaultPort;
    }

    @Override
    public void setKeySpace(String keySpace) {
        this.keySpace = keySpace;
    }

	@Override
	public Client getCassandraClient() throws Exception {
		throw new NotImplementedException("TODO");
	}

	@Override
	public void shutdown() {
		
	}

	@Override
	public void connect() {
		throw new NotImplementedException("TODO");		
	}

}
