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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.persistence.PersistenceException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SuperColumn;
import org.wyki.cassandra.pelops.KeyDeletor;
import org.wyki.cassandra.pelops.Mutator;
import org.wyki.cassandra.pelops.Pelops;
import org.wyki.cassandra.pelops.Policy;
import org.wyki.cassandra.pelops.Selector;

import com.impetus.kundera.CassandraClient;

/**
 * Client implementation using Pelops. http://code.google.com/p/pelops/
 * 
 * @author animesh.kumar
 * @since 0.1
 */
public class PelopsClient implements CassandraClient {

	private static final String poolName = "Main";

	/** array of cassandra hosts. */
	private String[] contactNodes;

	/** default port. */
	private int defaultPort;

	/** keyspace. */
	private String keySpace;

	private boolean closed = false;

	/**
	 * default constructor.
	 */
	public PelopsClient() {
	}
	
	@Override
	public void connect () {
		Pelops.addPool(poolName, contactNodes, defaultPort, false, null, new Policy());
	}

	@Override
	public void shutdown() {
		Pelops.shutdown();
		closed = true;
	}

	public boolean isOpen() {
		return !closed;
	}

	@Override
	public void writeColumns(String columnFamily, String rowId, Column... columns) throws Exception {
		
		if (!isOpen()) throw new PersistenceException("PelopsClient is closed.");
		
		Mutator mutator = Pelops.createMutator(poolName, keySpace);
		mutator.writeColumns(rowId, columnFamily, Arrays.asList(columns));
		mutator.execute(ConsistencyLevel.ONE);
	}

	@Override
	public void writeSuperColumns(String columnFamily, String rowId, SuperColumn... superColumns) throws Exception {
		
		if (!isOpen()) throw new PersistenceException("PelopsClient is closed.");
		
		Mutator mutator = Pelops.createMutator(poolName, keySpace);

		for (SuperColumn sc : superColumns) {
			mutator.writeSubColumns(rowId, columnFamily, sc.getName(), sc.getColumns());
		}
		mutator.execute(ConsistencyLevel.ONE);
	}

	@Override
	public List<Column> loadColumns(String columnFamily, String rowId) throws Exception {
		
		if (!isOpen()) throw new PersistenceException("PelopsClient is closed.");
		
		Selector selector = Pelops.createSelector(poolName, keySpace);
		List<Column> columns = selector.getColumnsFromRow(rowId, columnFamily, Selector.newColumnsPredicateAll(true, 10), ConsistencyLevel.ONE);
		return columns;
	}

	@Override
	public Map<String, List<Column>> loadColumns(String columnFamily, String... rowIds) throws Exception {
		
		if (!isOpen()) throw new PersistenceException("PelopsClient is closed.");
		
		Selector selector = Pelops.createSelector(poolName, keySpace);
		Map<String, List<Column>> map = selector.getColumnsFromRows(Arrays.asList(rowIds), columnFamily, Selector.newColumnsPredicateAll(false, 1000),
				ConsistencyLevel.ONE);
		return map;
	}

	@Override
	public void delete(String columnFamily, String rowId) throws Exception {
		
		if (!isOpen()) throw new PersistenceException("PelopsClient is closed.");
		
		KeyDeletor keyDeletor = Pelops.createKeyDeletor(poolName, keySpace);
		keyDeletor.deleteColumnFamily(rowId, columnFamily, ConsistencyLevel.ONE);
	}

	@Override
	public List<SuperColumn> loadSuperColumns(String columnFamily, String rowId, String... superColumnNames) throws Exception {
		if (!isOpen()) throw new PersistenceException("PelopsClient is closed.");
		Selector selector = Pelops.createSelector(poolName, keySpace);
		List<SuperColumn> list = selector.getSuperColumnsFromRow(rowId, columnFamily, Selector.newColumnsPredicate(superColumnNames), ConsistencyLevel.ONE);
		return list;
	}

	@Override
	public Map<String, List<SuperColumn>> loadSuperColumns(String columnFamily, String... rowIds) throws Exception {
		
		if (!isOpen()) throw new PersistenceException("PelopsClient is closed.");
		
		Selector selector = Pelops.createSelector(poolName, keySpace);
		Map<String, List<SuperColumn>> map = selector.getSuperColumnsFromRows(Arrays.asList(rowIds), columnFamily,
				Selector.newColumnsPredicateAll(false, 1000), ConsistencyLevel.ONE);
		return map;
	}

	@Override
	public Cassandra.Client getCassandraClient() throws Exception {
		return Pelops.getDbConnPool(poolName).getConnection().getAPI();
	}

	/**
	 * Sets the key space.
	 * 
	 * @param keySpace
	 *            the keySpace to set
	 */
	@Override
	public void setKeySpace(String keySpace) {
		this.keySpace = keySpace;
	}

	/**
	 * Sets the contact nodes.
	 * 
	 * @param contactNodes
	 *            the contactNodes to set
	 */
	@Override
	public void setContactNodes(String... contactNodes) {
		this.contactNodes = contactNodes;
	}

	/**
	 * Sets the default port.
	 * 
	 * @param defaultPort
	 *            the defaultPort to set
	 */
	@Override
	public void setDefaultPort(int defaultPort) {
		this.defaultPort = defaultPort;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("PelopsClient [contactNodes=");
		builder.append(Arrays.toString(contactNodes));
		builder.append(", defaultPort=");
		builder.append(defaultPort);
		builder.append(", keySpace=");
		builder.append(keySpace);
		builder.append(", closed=");
		builder.append(closed);
		builder.append("]");
		return builder.toString();
	}
}
