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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import javax.persistence.PersistenceException;

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
import com.impetus.kundera.Constants;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.loader.DBType;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;

// TODO: Auto-generated Javadoc
/**
 * Client implementation using Pelops. http://code.google.com/p/pelops/
 * 
 * @author animesh.kumar
 * @since 0.1
 */
public class PelopsClient implements CassandraClient
{

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

    /**
     * default constructor.
     */
    public PelopsClient()
    {
    }

    /* @see com.impetus.kundera.CassandraClient#connect() */
    /* (non-Javadoc)
     * @see com.impetus.kundera.Client#connect()
     */
    @Override
    public final void connect()
    {
    }

    /* @see com.impetus.kundera.CassandraClient#shutdown() */
    /* (non-Javadoc)
     * @see com.impetus.kundera.Client#shutdown()
     */
    @Override
    public final void shutdown()
    {
        Pelops.shutdown();
        closed = true;
    }

    /**
     * Checks if is open.
     * 
     * @return true, if is open
     */
    public final boolean isOpen()
    {
        return !closed;
    }

    @Deprecated
    @Override
    public final void writeColumns(String keyspace, String columnFamily, String rowId,
            List<EntityMetadata.Column> columns, EnhancedEntity e) throws Exception
    {	
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
		
        PelopsClient.ThriftRow tf = toThriftRow(e, m, columnFamily);
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
            String rowId, EntityMetadata m) throws Exception
    {

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        configurePool(keyspace);
        Selector selector = Pelops.createSelector(poolName);
        List<Column> columns = selector.getColumnsFromRow(columnFamily, new Bytes(rowId.getBytes()),
                Selector.newColumnsPredicateAll(true, 10), ConsistencyLevel.ONE);
        E e;
        if (null == columns || columns.size() == 0)
        {
            e = null;
        }
        else
        {
            e = fromThriftRow(em, clazz, m, this.new ThriftRow(rowId, columnFamily, columns));
        }
        return e;
    }

    /*
     * @see com.impetus.kundera.CassandraClient#loadColumns(java.lang.String,
     * java.lang.String, java.lang.String[])
     */
    /* (non-Javadoc)
     * @see com.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.EntityManagerImpl, java.lang.Class, java.lang.String, java.lang.String, com.impetus.kundera.metadata.EntityMetadata, java.lang.String[])
     */
    @Override
    public final/* Map<String, List<Column>> */<E> List<E> loadColumns(EntityManagerImpl em, Class<E> clazz,
            String keyspace, String columnFamily, EntityMetadata m, String... rowIds) throws Exception
    {

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        configurePool(keyspace);
        Selector selector = Pelops.createSelector(poolName);
        /**
         * String columnFamily, List<Bytes> rowKeys, SlicePredicate
         * colPredicate, ConsistencyLevel cLevel
         */
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

            E e = fromThriftRow(em, clazz, m, this.new ThriftRow(id, columnFamily, columns));
            entities.add(e);
        }
        return entities;
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
     * java.lang.String, java.lang.String, java.lang.String[])
     */
    /* (non-Javadoc)
     * @see com.impetus.kundera.CassandraClient#loadSuperColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])
     */
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

    /* @see com.impetus.kundera.CassandraClient#getCassandraClient() */
    /* (non-Javadoc)
     * @see com.impetus.kundera.CassandraClient#getCassandraClient()
     */
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
    public class ThriftRow
    {

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
        public ThriftRow(String id, String columnFamilyName, List<Column> columns)
        {
            this.id = id;
            this.columnFamilyName = columnFamilyName;
            this.columns = columns;
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

    /**
     * From thrift row.
     *
     * @param <E> the element type
     * @param em the em
     * @param clazz the clazz
     * @param m the m
     * @param cr the cr
     * @return the e
     * @throws Exception the exception
     */
    private <E> E fromThriftRow(EntityManagerImpl em, Class<E> clazz, EntityMetadata m, PelopsClient.ThriftRow cr)
            throws Exception
    {

        // Instantiate a new instance
        E e = clazz.newInstance();

        // Set row-key. Note: @Id is always String.
        PropertyAccessorHelper.set(e, m.getIdProperty(), cr.getId());

        // Iterate through each column
        for (Column c : cr.getColumns())
        {
            String name = PropertyAccessorFactory.STRING.fromBytes(c.getName());
            byte[] value = c.getValue();

            if (null == value)
            {
                continue;
            }

            // check if this is a property?
            EntityMetadata.Column column = m.getColumn(name);
            if (null == column)
            {
                // it could be some relational column
                EntityMetadata.Relation relation = m.getRelation(name);

                if (relation == null)
                {
                    continue;
                }

                String foreignKeys = PropertyAccessorFactory.STRING.fromBytes(value);
                Set<String> keys = deserializeKeys(foreignKeys);
                em.getEntityResolver().populateForeignEntities(e, cr.getId(), relation, keys.toArray(new String[0]));
            }

            else
            {
                try
                {
                    PropertyAccessorHelper.set(e, column.getField(), value);
                }
                catch (PropertyAccessException pae)
                {
                    log.warn(pae.getMessage());
                }
            }
        }

        return e;
    }

    /**
     * Helper method to convert @Entity to ThriftRow.
     *
     * @param e the e
     * @param columnsLst the columns lst
     * @param columnFamily the colmun family
     * @return the base data accessor. thrift row
     * @throws Exception the exception
     */
    private PelopsClient.ThriftRow toThriftRow(EnhancedEntity e, EntityMetadata m, String columnFamily) 
    	throws Exception {    	
    	// timestamp to use in thrift column objects
        long timestamp = System.currentTimeMillis();

        PelopsClient.ThriftRow tr = this.new ThriftRow();

        tr.setColumnFamilyName(columnFamily);	        		// column-family name       
        tr.setId(e.getId());									// Id
        
        addSuperColumnsToThriftRow(timestamp, tr, m, e);		//Super columns  
        
        if(m.getSuperColumnsAsList().isEmpty()) {
        	addColumnsToThriftRow(timestamp, tr, m, e);				//Columns
        }        

        return tr;
    }
        
    private void addColumnsToThriftRow(long timestamp, PelopsClient.ThriftRow tr, EntityMetadata m, EnhancedEntity e) throws Exception {
    	List<Column> columns = new ArrayList<Column>();
    	
        // Iterate through each column-meta and populate that with field values
        for (EntityMetadata.Column column : m.getColumnsAsList()) {
            Field field = column.getField();
            String name = column.getName();
            try {
                byte[] value = PropertyAccessorHelper.get(e.getEntity(), field);
                Column col = new Column();
                col.setName(PropertyAccessorFactory.STRING.toBytes(name));
                col.setValue(value);
                col.setTimestamp(timestamp);
                columns.add(col);
            } catch (PropertyAccessException exp) {
                log.warn(exp.getMessage());
            }

        }

        // add foreign keys
        for (Map.Entry<String, Set<String>> entry : e.getForeignKeysMap().entrySet()) {
            String property = entry.getKey();
            Set<String> foreignKeys = entry.getValue();

            String keys = serializeKeys(foreignKeys);
            if (null != keys) {
                Column col = new Column();

                col.setName(PropertyAccessorFactory.STRING.toBytes(property));
                col.setValue(PropertyAccessorFactory.STRING.toBytes(keys));
                col.setTimestamp(timestamp);
                columns.add(col);
            }
        }
        tr.setColumns(columns);			//Columns
    }
    
    private void addSuperColumnsToThriftRow(long timestamp, PelopsClient.ThriftRow tr, EntityMetadata m, EnhancedEntity e) throws Exception {
    	 //Iterate through Super columns
        for (EntityMetadata.SuperColumn superColumn : m.getSuperColumnsAsList()) {            
            Field superColumnField = superColumn.getField();
            Object superColumnObject = PropertyAccessorHelper.getObject(e.getEntity(), superColumnField);
             
            
            //If Embedded object is a Collection, there will be variable number of super columns one for each object in collection.
            //Key for each super column will be of the format "<Embedded object field name>#<Unique sequence number>
            
            //On the other hand, if embedded object is not a Collection, it would simply be embedded as ONE super column.
            if(superColumnObject instanceof Collection) {
            	for(Object obj : (Collection)superColumnObject) {
            		superColumn.setName(UUID.randomUUID().toString());		//Change this to correct format
            		SuperColumn thriftSuperColumn = buildThriftSuperColumn(timestamp, superColumn, obj);
            		tr.addSuperColumn(thriftSuperColumn);
            	}
            	
            } else {
            	SuperColumn thriftSuperColumn = buildThriftSuperColumn(timestamp, superColumn, superColumnObject);
                tr.addSuperColumn(thriftSuperColumn);            	
            }         
            
        }
    }
    
    private SuperColumn buildThriftSuperColumn(long timestamp, EntityMetadata.SuperColumn superColumn, Object superColumnObject) throws PropertyAccessException {
    	List<Column> thriftColumns = new ArrayList<Column>();  
    	for (EntityMetadata.Column column : superColumn.getColumns()) {
            Field field = column.getField();
            String name = column.getName();

            try {
                byte[] value = PropertyAccessorHelper.get(superColumnObject, field);
                if (null != value) {
                    Column thriftColumn = new Column();
                    thriftColumn.setName(PropertyAccessorFactory.STRING.toBytes(name));
                    thriftColumn.setValue(value);
                    thriftColumn.setTimestamp(timestamp);
                    thriftColumns.add(thriftColumn);
                }
            } catch (PropertyAccessException exp) {
                log.warn(exp.getMessage());
            }
        }
        SuperColumn thriftSuperColumn = new SuperColumn();        
        thriftSuperColumn.setName(PropertyAccessorFactory.STRING.toBytes(superColumn.getName()));
        thriftSuperColumn.setColumns(thriftColumns);      
        
        return thriftSuperColumn;
    }

    /**
     * Splits foreign keys into Set.
     * 
     * @param foreignKeys
     *            the foreign keys
     * @return the set
     */
    private Set<String> deserializeKeys(String foreignKeys)
    {
        Set<String> keys = new HashSet<String>();

        if (null == foreignKeys || foreignKeys.isEmpty())
        {
            return keys;
        }

        String array[] = foreignKeys.split(Constants.SEPARATOR);
        for (String element : array)
        {
            keys.add(element);
        }
        return keys;
    }

    /**
     * Creates a string representation of a set of foreign keys by combining
     * them together separated by "~" character.
     * 
     * Note: Assumption is that @Id will never contain "~" character. Checks for
     * this are not added yet.
     * 
     * @param foreignKeys
     *            the foreign keys
     * @return the string
     */
    protected String serializeKeys(Set<String> foreignKeys)
    {
        if (null == foreignKeys || foreignKeys.isEmpty())
        {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (String key : foreignKeys)
        {
            if (sb.length() > 0)
            {
                sb.append(Constants.SEPARATOR);
            }
            sb.append(key);
        }
        return sb.toString();
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
     *
     * @param keyspace the keyspace
     */
    private void configurePool(String keyspace)
    {
        Cluster cluster = new Cluster(contactNodes, new IConnection.Config(defaultPort, true, -1), false);
        Pelops.addPool(poolName, cluster, keyspace);
    }
    
}
