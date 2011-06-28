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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import javax.persistence.PersistenceException;

import lucandra.CassandraUtils;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.IConnection;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.RowDeletor;
import org.scale7.cassandra.pelops.Selector;

import com.impetus.kundera.CassandraClient;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.loader.DBType;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.EmbeddedCollectionCacheHandler;
import com.impetus.kundera.pelops.PelopsDataHandler;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * Client implementation using Pelops. http://code.google.com/p/pelops/ 
 * @author animesh.kumar
 * @since 0.1
 */
public class PelopsClient implements CassandraClient {

    /** The Constant poolName. */
    private static final String poolName = "Main";

    /** array of cassandra hosts. */
    private String[] contactNodes;

    /** default port. */
    private int defaultPort;

    /** The closed. */
    private boolean closed = false;

    /** log for this class. */
    private static Log log = LogFactory.getLog(PelopsClient.class);
    
    PelopsDataHandler dataHandler = new PelopsDataHandler();  
   
    
    EmbeddedCollectionCacheHandler ecCacheHandler = new EmbeddedCollectionCacheHandler();
    

    /**
     * default constructor.
     */
    public PelopsClient() {
    	
    }


    @Override
    public final void connect() {
    	//new SolandraUtils().startSolandraServer();
    }


    @Override
    public final void shutdown()
    {
        Pelops.shutdown();
        closed = true;
    }

    /**
     * Checks if is open. 
     * @return true, if is open
     */
    public final boolean isOpen()
    {
        return !closed;
    }

    @Deprecated
    @Override
    public final void writeColumns(String keyspace, String columnFamily, String rowId,
            List<EntityMetadata.Column> columns, EnhancedEntity e) throws Exception {	
    	throw new PersistenceException("Not yet implemented");        
    }


    @Override
    public void writeColumns(EntityManagerImpl em, EnhancedEntity e, EntityMetadata m) 
    	throws Exception {
    	
		String keyspace = m.getSchema();
		String columnFamily = m.getTableName();
		
		if (!isOpen()) {
            throw new PersistenceException("PelopsClient is closed.");
        }
		
        PelopsClient.ThriftRow tf = dataHandler.toThriftRow(this, e, m, columnFamily);
        configurePool(keyspace);

        Mutator mutator = Pelops.createMutator(poolName);
        
        
        List<Column> thriftColumns = tf.getColumns();
        List<SuperColumn> thriftSuperColumns = tf.getSuperColumns();
        if(thriftColumns != null && ! thriftColumns.isEmpty()) {
        	mutator.writeColumns(columnFamily, new Bytes(tf.getId().getBytes()),
                    Arrays.asList(tf.getColumns().toArray(new Column[0])));
        }
        
        if(thriftSuperColumns != null && ! thriftSuperColumns.isEmpty()) {
        	for (SuperColumn sc : thriftSuperColumns) {
                Bytes.toUTF8(sc.getColumns().get(0).getValue());
                mutator.writeSubColumns(columnFamily, tf.getId(), Bytes.toUTF8(sc.getName()), sc.getColumns());            
                
                
                
            }       	
        	
        }      
        
        mutator.execute(ConsistencyLevel.ONE);
        
    }

    @Override
    @Deprecated
    public final void writeSuperColumns(String keyspace, String columnFamily, String rowId, SuperColumn... superColumns)
            throws Exception
    {

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        configurePool(keyspace);
        Mutator mutator = Pelops.createMutator(poolName);

        for (SuperColumn sc : superColumns)
        {
            /**
             * String colFamily, String rowKey, String colName, List<Column>
             * subColumns
             */
            Bytes.toUTF8(sc.getColumns().get(0).getValue());
            mutator.writeSubColumns(columnFamily, rowId, Bytes.toUTF8(sc.getName()), sc.getColumns());
        }
        mutator.execute(ConsistencyLevel.ONE);
    }

    /*
     * @see com.impetus.kundera.CassandraClient#loadColumns(java.lang.String,
     * java.lang.String, java.lang.String)
     */
    /* (non-Javadoc)
     * @see com.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.EntityManagerImpl, java.lang.Class, java.lang.String, java.lang.String, java.lang.String, com.impetus.kundera.metadata.EntityMetadata)
     */
    @Override
    public final <E> E loadColumns(EntityManagerImpl em, Class<E> clazz, String keyspace, String columnFamily,
            String rowId, EntityMetadata m) throws Exception {

        if (!isOpen()) {
            throw new PersistenceException("PelopsClient is closed.");
        }

        configurePool(keyspace);
        Selector selector = Pelops.createSelector(poolName);      
               
        
        E e = dataHandler.fromThriftRow(selector, em, clazz, m, rowId);         
        
        return e;
    }

    @Override
    public final <E> List<E> loadColumns(EntityManagerImpl em, Class<E> clazz,
            String keyspace, String columnFamily, EntityMetadata m, String... rowIds) throws Exception {

		if (!isOpen()) {
			throw new PersistenceException("PelopsClient is closed.");
		}

        configurePool(keyspace);
        Selector selector = Pelops.createSelector(poolName); 
        
        
        
        List<Bytes> bytesArr = new ArrayList<Bytes>();

        for (String rowkey : rowIds)
        {
            Bytes bytes = new Bytes(PropertyAccessorFactory.STRING.toBytes(rowkey));
            bytesArr.add(bytes);
        }

        Map<Bytes, List<Column>> map = selector.getColumnsFromRows(columnFamily, bytesArr,
                Selector.newColumnsPredicateAll(false, 1000), ConsistencyLevel.ONE);
        List<E> entities = new ArrayList<E>();
        // Iterate and populate entities
        for (Map.Entry<Bytes, List<Column>> entry : map.entrySet())
        {

            String id = PropertyAccessorFactory.STRING.fromBytes(entry.getKey().toByteArray());

            List<Column> columns = entry.getValue();

            if (entry.getValue().size() == 0)
            {
                log.debug("@Entity not found for id: " + id);
                continue;
            }

            E e = dataHandler.fromColumnThriftRow(em, clazz, m, this.new ThriftRow(id, columnFamily, columns, null));
            entities.add(e);
        }
        return entities;
    }
    
    @Deprecated
    @Override
    public final List<SuperColumn> loadSuperColumns(String keyspace, String columnFamily, String rowId,
            String... superColumnNames) throws Exception
    {
        if (!isOpen())
            throw new PersistenceException("PelopsClient is closed.");
        configurePool(keyspace);
        Selector selector = Pelops.createSelector(poolName);
        return selector.getSuperColumnsFromRow(columnFamily, rowId, Selector.newColumnsPredicate(superColumnNames),
                ConsistencyLevel.ONE);
    }
    
    public <E> List<E> loadColumns(EntityManagerImpl em, EntityMetadata m, Queue filterClauseQueue) throws Exception {
    	throw new NotImplementedException("Not yet implemented");
    }

    /*
     * @see com.impetus.kundera.CassandraClient#delete(java.lang.String,
     * java.lang.String, java.lang.String)
     */
    /* (non-Javadoc)
     * @see com.impetus.kundera.Client#delete(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public final void delete(String keyspace, String columnFamily, String rowId) throws Exception
    {

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        configurePool(keyspace);
        RowDeletor rowDeletor = Pelops.createRowDeletor(poolName);
        rowDeletor.deleteRow(columnFamily, rowId, ConsistencyLevel.ONE);
    }

    /*
     * @see
     * com.impetus.kundera.CassandraClient#loadSuperColumns(java.lang.String,
     * java.lang.String, java.lang.String[])
     */
    /* (non-Javadoc)
     * @see com.impetus.kundera.CassandraClient#loadSuperColumns(java.lang.String, java.lang.String, java.lang.String[])
     */
    @Override
    public final Map<Bytes, List<SuperColumn>> loadSuperColumns(String keyspace, String columnFamily, String... rowIds)
            throws Exception
    {

        if (!isOpen())
            throw new PersistenceException("PelopsClient is closed.");

        configurePool(keyspace);
        Selector selector = Pelops.createSelector(poolName);

        List<Bytes> bytesArr = new ArrayList<Bytes>();

        for (String rowkey : rowIds)
        {
            Bytes bytes = new Bytes(PropertyAccessorFactory.STRING.toBytes(rowkey));
            bytesArr.add(bytes);
        }
        /**
         * String columnFamily, List<Bytes> rowKeys, SlicePredicate
         * colPredicate, ConsistencyLevel cLevel
         */
        return selector.getSuperColumnsFromRows(columnFamily, bytesArr, Selector.newColumnsPredicateAll(false, 1000),
                ConsistencyLevel.ONE);
    }

    
    @Override
    public final Cassandra.Client getCassandraClient() throws Exception
    {
        return Pelops.getDbConnPool(poolName).getConnection().getAPI();
    }

    /**
     * Sets the contact nodes.
     * 
     * @param contactNodes
     *            the contactNodes to set
     */
    @Override
    public final void setContactNodes(String... contactNodes)
    {
        this.contactNodes = contactNodes;
    }

    /**
     * Sets the default port.
     * 
     * @param defaultPort
     *            the defaultPort to set
     */
    @Override
    public final void setDefaultPort(int defaultPort)
    {
        this.defaultPort = defaultPort;
    }

    /* @see java.lang.Object#toString() */
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public final String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("PelopsClient [contactNodes=");
        builder.append(Arrays.toString(contactNodes));
        builder.append(", defaultPort=");
        builder.append(defaultPort);
        builder.append(", closed=");
        builder.append(closed);
        builder.append("]");
        return builder.toString();
    }

    /**
     * Utility class that represents a row in Cassandra DB.
     * 
     * @author animesh.kumar
     */
    public class ThriftRow {

        /** Id of the row. */
        private String id;

        /** name of the family. */
        private String columnFamilyName;

        /** list of thrift columns from the row. */
        private List<Column> columns;
        
        /** list of thrift super columns columns from the row. */
        private List<SuperColumn> superColumns;      
        

        /**
         * default constructor.
         */
        public ThriftRow()
        {
            columns = new ArrayList<Column>();
            superColumns = new ArrayList<SuperColumn>();
        }

        /**
         * The Constructor.
         * 
         * @param id
         *            the id
         * @param columnFamilyName
         *            the column family name
         * @param columns
         *            the columns
         */
        public ThriftRow(String id, String columnFamilyName, List<Column> columns, List<SuperColumn> superColumns) {
            this.id = id;
            this.columnFamilyName = columnFamilyName;
            if(columns != null) {
            	this.columns = columns;
            }
            
            if(superColumns != null) {
            	this.superColumns = superColumns;
            }
            
        }        
        

        /**
         * Gets the id.
         * 
         * @return the id
         */
        public String getId()
        {
            return id;
        }

        /**
         * Sets the id.
         * 
         * @param id
         *            the key to set
         */
        public void setId(String id)
        {
            this.id = id;
        }

        /**
         * Gets the column family name.
         * 
         * @return the columnFamilyName
         */
        public String getColumnFamilyName()
        {
            return columnFamilyName;
        }

        /**
         * Sets the column family name.
         * 
         * @param columnFamilyName
         *            the columnFamilyName to set
         */
        public void setColumnFamilyName(String columnFamilyName)
        {
            this.columnFamilyName = columnFamilyName;
        }

        /**
         * Gets the columns.
         * 
         * @return the columns
         */
        public List<Column> getColumns()
        {
            return columns;
        }

        /**
         * Sets the columns.
         * 
         * @param columns
         *            the columns to set
         */
        public void setColumns(List<Column> columns)
        {
            this.columns = columns;
        }

        /**
         * Adds the column.
         * 
         * @param column
         *            the column
         */
        public void addColumn(Column column)
        {
            columns.add(column);
        }

		/**
		 * @return the superColumns
		 */
		public List<SuperColumn> getSuperColumns() {
			return superColumns;
		}

		/**
		 * @param superColumns the superColumns to set
		 */
		public void setSuperColumns(List<SuperColumn> superColumns) {
			this.superColumns = superColumns;
		}
		
		/**
		 * @param superColumns the superColumns to set
		 */
		public void addSuperColumn(SuperColumn superColumn) {
			this.superColumns.add(superColumn);
		}    
        
    }

    

   

    /* (non-Javadoc)
     * @see com.impetus.kundera.Client#setKeySpace(java.lang.String)
     */
    @Override
    public void setKeySpace(String keySpace)
    {

    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.Client#getType()
     */
    @Override
    public DBType getType()
    {
        return DBType.CASSANDRA;
    }

   
    /**
     * Configure pool.
     * @param keyspace the keyspace
     */
    private void configurePool(String keyspace) {
        Cluster cluster = new Cluster(contactNodes, new IConnection.Config(defaultPort, true, -1), false);
        Pelops.addPool(poolName, cluster, keyspace);
    }


	/**
	 * @return the scCacheHandler
	 */
	public EmbeddedCollectionCacheHandler getEcCacheHandler() {
		return ecCacheHandler;
	}
	
	
	private class SolandraUtils {
		public void startSolandraServer() {
			log.info("Starting Solandra Server.");
			CassandraUtils.cacheInvalidationInterval = 0; // real-time

			try {
				// Load solandra specific schema
				CassandraUtils.setStartup();				
				CassandraUtils.createCassandraSchema();
				
			} catch (Throwable t) {
				log.error("errror while starting solandra schema:", t);
			}

		}
	}
    
}
