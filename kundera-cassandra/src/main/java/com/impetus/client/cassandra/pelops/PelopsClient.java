/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/

package com.impetus.client.cassandra.pelops;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.Query;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.IConnection;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.RowDeletor;
import org.scale7.cassandra.pelops.Selector;

import com.impetus.client.cassandra.index.SolandraIndexer;
import com.impetus.kundera.Constants;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.DBType;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.index.Indexer;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.EntityResolver;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;
import com.impetus.kundera.query.LuceneQuery;

/**
 * Client implementation using Pelops. http://code.google.com/p/pelops/
 * 
 * @author animesh.kumar
 * @since 0.1
 */
public class PelopsClient implements Client
{

    /** The Constant poolName. */
    private static final String POOL_NAME = "Main";

    /** array of cassandra hosts. */
    private String[] contactNodes;

    /** default port. */
    private int defaultPort;

    /** The closed. */
    private boolean closed = false;

    /** log for this class. */
    private static Log log = LogFactory.getLog(PelopsClient.class);

    /** The data handler. */
    PelopsDataHandler dataHandler = new PelopsDataHandler(this);

    /** Whether this client is connected */
    private boolean isConnected;

    /** The index manager. */
    private IndexManager indexManager;

    private EntityResolver entityResolver;

    private String persistenceUnit;

    private Indexer indexer;

    /**
     * default constructor.
     */
    public PelopsClient()
    {
        indexer = new SolandraIndexer(this, new StandardAnalyzer(Version.LUCENE_CURRENT));
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#connect()
     */
    @Override
    public final void connect()
    {

    }

    /*
     * (non-Javadoc)
     * 
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

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#writeColumns(com.impetus.kundera.ejb.
     * EntityManager, com.impetus.kundera.proxy.EnhancedEntity,
     * com.impetus.kundera.metadata.EntityMetadata)
     */
    @Override
    public void writeData(EnhancedEntity enhancedEntity) throws Exception
    {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), enhancedEntity
                .getEntity().getClass());
        String keyspace = entityMetadata.getSchema();
        String columnFamily = entityMetadata.getTableName();

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        PelopsClient.ThriftRow tf = dataHandler.toThriftRow(this, enhancedEntity, entityMetadata, columnFamily);
        configurePool(keyspace);

        Mutator mutator = Pelops.createMutator(POOL_NAME);

        List<Column> thriftColumns = tf.getColumns();
        List<SuperColumn> thriftSuperColumns = tf.getSuperColumns();
        if (thriftColumns != null && !thriftColumns.isEmpty())
        {
            mutator.writeColumns(columnFamily, new Bytes(tf.getId().getBytes()),
                    Arrays.asList(tf.getColumns().toArray(new Column[0])));
        }

        if (thriftSuperColumns != null && !thriftSuperColumns.isEmpty())
        {
            for (SuperColumn sc : thriftSuperColumns)
            {
                Bytes.toUTF8(sc.getColumns().get(0).getValue());
                mutator.writeSubColumns(columnFamily, tf.getId(), Bytes.toUTF8(sc.getName()), sc.getColumns());

            }

        }
        mutator.execute(ConsistencyLevel.ONE);

        getIndexManager().update(entityMetadata, enhancedEntity.getEntity());

    }

    @Override
    public final <E> E loadData(Class<E> entityClass, String rowId) throws Exception
    {

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        configurePool(entityMetadata.getSchema());
        Selector selector = Pelops.createSelector(POOL_NAME);

        E e = (E) dataHandler.fromThriftRow(selector, entityClass, entityMetadata, rowId);

        return e;
    }

    @Override
    public final <E> List<E> loadData(Class<E> entityClass, String... rowIds) throws Exception
    {
        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        configurePool(entityMetadata.getSchema());
        Selector selector = Pelops.createSelector(POOL_NAME);

        List<E> entities = (List<E>) dataHandler.fromThriftRow(selector, entityClass, entityMetadata, rowIds);

        return entities;
    }

    @Override
    public <E> List<E> loadData(Class<E> entityClass, Map<String, String> col) throws Exception
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), entityClass);
        List<E> entities = new ArrayList<E>();
        for (String superColName : col.keySet())
        {
            String entityId = col.get(superColName);
            List<SuperColumn> superColumnList = loadSuperColumns(entityMetadata.getSchema(),
                    entityMetadata.getTableName(), entityId, new String[] { superColName });
            E e = (E) fromThriftRow(entityMetadata.getEntityClazz(), entityMetadata, new DataRow<SuperColumn>(entityId,
                    entityMetadata.getTableName(), superColumnList));
            entities.add(e);
        }
        return entities;
    }

    public final List<SuperColumn> loadSuperColumns(String keyspace, String columnFamily, String rowId,
            String... superColumnNames) throws Exception
    {
        if (!isOpen())
            throw new PersistenceException("PelopsClient is closed.");
        configurePool(keyspace);
        Selector selector = Pelops.createSelector(POOL_NAME);
        return selector.getSuperColumnsFromRow(columnFamily, rowId, Selector.newColumnsPredicate(superColumnNames),
                ConsistencyLevel.ONE);
    }

    @Override
    public <E> List<E> loadData(Query query) throws Exception
    {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public final void delete(EnhancedEntity enhancedEntity) throws Exception
    {

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(getPersistenceUnit(), enhancedEntity
                .getEntity().getClass());
        configurePool(entityMetadata.getSchema());
        RowDeletor rowDeletor = Pelops.createRowDeletor(POOL_NAME);
        rowDeletor.deleteRow(entityMetadata.getTableName(), enhancedEntity.getId(), ConsistencyLevel.ONE);
        getIndexManager().remove(entityMetadata, enhancedEntity.getEntity(), enhancedEntity.getId());
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
    /*
     * (non-Javadoc)
     * 
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
         * @param superColumns
         *            the super columns
         */
        public ThriftRow(String id, String columnFamilyName, List<Column> columns, List<SuperColumn> superColumns)
        {
            this.id = id;
            this.columnFamilyName = columnFamilyName;
            if (columns != null)
            {
                this.columns = columns;
            }

            if (superColumns != null)
            {
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
         * Gets the super columns.
         * 
         * @return the superColumns
         */
        public List<SuperColumn> getSuperColumns()
        {
            return superColumns;
        }

        /**
         * Sets the super columns.
         * 
         * @param superColumns
         *            the superColumns to set
         */
        public void setSuperColumns(List<SuperColumn> superColumns)
        {
            this.superColumns = superColumns;
        }

        /**
         * Adds the super column.
         * 
         * @param superColumn
         *            the super column
         */
        public void addSuperColumn(SuperColumn superColumn)
        {
            this.superColumns.add(superColumn);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#setKeySpace(java.lang.String)
     */
    @Override
    public void setSchema(String keySpace)
    {

    }

    /*
     * (non-Javadoc)
     * 
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
     * @param keyspace
     *            the keyspace
     */
    private void configurePool(String keyspace)
    {
        Cluster cluster = new Cluster(contactNodes, new IConnection.Config(defaultPort, true, -1), false);
        Pelops.addPool(POOL_NAME, cluster, keyspace);

    }

    /**
     * From thrift row.
     * 
     * @param <E>
     *            the element type
     * @param clazz
     *            the clazz
     * @param m
     *            the m
     * @param tr
     *            the cr
     * @return the e
     * @throws Exception
     *             the exception
     */
    // TODO: this is a duplicate code snippet and we need to refactor this.(it
    // should be moved to PelopsDataHandler)
    private <E> E fromThriftRow(Class<E> clazz, EntityMetadata m, DataRow<SuperColumn> tr) throws Exception
    {

        // Instantiate a new instance
        E e = clazz.newInstance();

        // Set row-key. Note: @Id is always String.
        PropertyAccessorHelper.set(e, m.getIdColumn().getField(), tr.getId());

        // Get a name->field map for super-columns
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
        Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();
        MetadataUtils.populateColumnAndSuperColumnMaps(m, columnNameToFieldMap, superColumnNameToFieldMap);

        Collection embeddedCollection = null;
        Field embeddedCollectionField = null;
        for (SuperColumn sc : tr.getColumns())
        {

            String scName = PropertyAccessorFactory.STRING.fromBytes(sc.getName());
            String scNamePrefix = null;

            if (scName.indexOf(Constants.EMBEDDED_COLUMN_NAME_DELIMITER) != -1)
            {
                scNamePrefix = MetadataUtils.getEmbeddedCollectionPrefix(scName);
                embeddedCollectionField = superColumnNameToFieldMap.get(scNamePrefix);
                embeddedCollection = MetadataUtils.getEmbeddedCollectionInstance(embeddedCollectionField);

                String scFieldName = scName.substring(0, scName.indexOf(Constants.EMBEDDED_COLUMN_NAME_DELIMITER));
                Field superColumnField = e.getClass().getDeclaredField(scFieldName);
                if (!superColumnField.isAccessible())
                {
                    superColumnField.setAccessible(true);
                }
                // Collection embeddedCollection = null;
                if (superColumnField.getType().equals(List.class))
                {
                    embeddedCollection = new ArrayList();
                }
                else if (superColumnField.getType().equals(Set.class))
                {
                    embeddedCollection = new HashSet();
                }
                PelopsDataHandler handler = new PelopsDataHandler(this);
                Object embeddedObject = handler.populateEmbeddedObject(sc, m);
                embeddedCollection.add(embeddedObject);
                superColumnField.set(e, embeddedCollection);
            }
            else
            {
                boolean intoRelations = false;
                if (scName.equals(Constants.FOREIGN_KEY_EMBEDDED_COLUMN_NAME))
                {
                    intoRelations = true;
                }

                for (Column column : sc.getColumns())
                {
                    String name = PropertyAccessorFactory.STRING.fromBytes(column.getName());
                    byte[] value = column.getValue();

                    if (value == null)
                    {
                        continue;
                    }

                    if (intoRelations)
                    {
                        Relation relation = m.getRelation(name);

                        String foreignKeys = PropertyAccessorFactory.STRING.fromBytes(value);
                        Set<String> keys = MetadataUtils.deserializeKeys(foreignKeys);
                        getEntityResolver().populateForeignEntities(e, tr.getId(), relation,
                                keys.toArray(new String[0]));

                    }
                    else
                    {
                        // set value of the field in the bean
                        Field field = columnNameToFieldMap.get(name);
                        Object embeddedObject = PropertyAccessorHelper.getObject(e, scName);
                        PropertyAccessorHelper.set(embeddedObject, field, value);
                    }
                }
            }
        }
        return e;
    }

    @Override
    public Indexer getIndexer()
    {
        return indexer;
    }

    /**
     * Gets the index manager.
     * 
     * @return the indexManager
     */
    @Override
    public final IndexManager getIndexManager()
    {
        return indexManager;
    }

    @Override
    public Query getQuery(String ejbqlString)
    {
        return new LuceneQuery(this, ejbqlString);
    }

    @Override
    public String getPersistenceUnit()
    {
        return persistenceUnit;
    }

    @Override
    public EntityResolver getEntityResolver()
    {
        return entityResolver;
    }

    // TODO To remove the setters

    public void setIndexManager(IndexManager indexManager)
    {
        this.indexManager = indexManager;
    }

    public void setEntityResolver(EntityResolver entityResolver)
    {
        this.entityResolver = entityResolver;
    }

    public void setPersistenceUnit(String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
    }

}
