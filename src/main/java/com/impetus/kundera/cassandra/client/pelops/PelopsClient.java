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
package com.impetus.kundera.cassandra.client.pelops;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import javax.persistence.PersistenceException;

import lucandra.CassandraUtils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.IConnection;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.RowDeletor;
import org.scale7.cassandra.pelops.Selector;

import com.impetus.kundera.cassandra.client.CassandraClient;
import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.loader.DBType;
import com.impetus.kundera.metadata.EmbeddedCollectionCacheHandler;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * Client implementation using Pelops. http://code.google.com/p/pelops/
 * 
 * @author animesh.kumar
 * @since 0.1
 */
public class PelopsClient implements CassandraClient
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
    PelopsDataHandler dataHandler = new PelopsDataHandler();

    /** The ec cache handler. */
    EmbeddedCollectionCacheHandler ecCacheHandler = new EmbeddedCollectionCacheHandler();

    /**
     * default constructor.
     */
    public PelopsClient()
    {
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#connect()
     */
    @Override
    public final void connect()
    {
        // Start Solandra Service
        new SolandraUtils().startSolandraServer();
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
     * @see com.impetus.kundera.Client#writeColumns(java.lang.String,
     * java.lang.String, java.lang.String, java.util.List,
     * com.impetus.kundera.proxy.EnhancedEntity)
     */
    @Deprecated
    @Override
    public final void writeColumns(String keyspace, String columnFamily, String rowId,
            List<EntityMetadata.Column> columns, EnhancedEntity e) throws Exception
    {
        throw new PersistenceException("Not yet implemented");
    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#writeColumns(com.impetus.kundera.ejb.
     * EntityManagerImpl, com.impetus.kundera.proxy.EnhancedEntity,
     * com.impetus.kundera.metadata.EntityMetadata)
     */
    @Override
    public void writeColumns(EntityManagerImpl em, EnhancedEntity e, EntityMetadata m) throws Exception
    {

        String keyspace = m.getSchema();
        String columnFamily = m.getTableName();

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        PelopsClient.ThriftRow tf = dataHandler.toThriftRow(this, e, m, columnFamily);
        configurePool(keyspace);

        Mutator mutator = Pelops.createMutator(POOL_NAME);
        System.out.println(Pelops.getDbConnPool(POOL_NAME).getKeyspace());

        List<Column> thriftColumns = tf.getColumns();
        List<SuperColumn> thriftSuperColumns = tf.getSuperColumns();
        if (thriftColumns != null && !thriftColumns.isEmpty())
        {
            mutator.writeColumns(columnFamily, new Bytes(tf.getId().getBytes()), Arrays.asList(tf.getColumns().toArray(
                    new Column[0])));
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

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.CassandraClient#writeSuperColumns(java.lang.String,
     * java.lang.String, java.lang.String,
     * org.apache.cassandra.thrift.SuperColumn[])
     */
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
        Mutator mutator = Pelops.createMutator(POOL_NAME);

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
    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManagerImpl, java.lang.Class, java.lang.String, java.lang.String,
     * java.lang.String, com.impetus.kundera.metadata.EntityMetadata)
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
        Selector selector = Pelops.createSelector(POOL_NAME);

        E e = dataHandler.fromThriftRow(selector, em, clazz, m, rowId);

        return e;
    }

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManagerImpl, java.lang.Class, java.lang.String, java.lang.String,
     * com.impetus.kundera.metadata.EntityMetadata, java.lang.String[])
     */
    @Override
    // TODO we need to refactor/reimplement this.
    public final <E> List<E> loadColumns(EntityManagerImpl em, Class<E> clazz, String keyspace, String columnFamily,
            EntityMetadata m, String... rowIds) throws Exception
    {

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        configurePool(keyspace);
        Selector selector = Pelops.createSelector(POOL_NAME);

        List<Bytes> bytesArr = new ArrayList<Bytes>();

        for (String rowkey : rowIds)
        {
            Bytes bytes = new Bytes(PropertyAccessorFactory.STRING.toBytes(rowkey));
            bytesArr.add(bytes);
        }

        Map<Bytes, List<Column>> map = selector.getColumnsFromRows(columnFamily, bytesArr, Selector
                .newColumnsPredicateAll(false, 1000), ConsistencyLevel.ONE);
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

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.CassandraClient#loadSuperColumns(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String[])
     */
    @Override
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

    /*
     * (non-Javadoc)
     * 
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManagerImpl, com.impetus.kundera.metadata.EntityMetadata,
     * java.util.Queue)
     */
    public <E> List<E> loadColumns(EntityManagerImpl em, EntityMetadata m, Queue filterClauseQueue) throws Exception
    {
        throw new NotImplementedException("Not yet implemented");
    }

    /*
     * @see com.impetus.kundera.CassandraClient#delete(java.lang.String,
     * java.lang.String, java.lang.String)
     */
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.Client#delete(java.lang.String,
     * java.lang.String, java.lang.String)
     */
    @Override
    public final void delete(String keyspace, String columnFamily, String rowId) throws Exception
    {

        if (!isOpen())
        {
            throw new PersistenceException("PelopsClient is closed.");
        }

        configurePool(keyspace);
        RowDeletor rowDeletor = Pelops.createRowDeletor(POOL_NAME);
        rowDeletor.deleteRow(columnFamily, rowId, ConsistencyLevel.ONE);
    }

    /*
     * @see
     * com.impetus.kundera.CassandraClient#loadSuperColumns(java.lang.String,
     * java.lang.String, java.lang.String[])
     */
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.CassandraClient#loadSuperColumns(java.lang.String,
     * java.lang.String, java.lang.String[])
     */
    @Override
    public final Map<Bytes, List<SuperColumn>> loadSuperColumns(String keyspace, String columnFamily, String... rowIds)
            throws Exception
    {

        if (!isOpen())
            throw new PersistenceException("PelopsClient is closed.");

        configurePool(keyspace);
        Selector selector = Pelops.createSelector(POOL_NAME);

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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.CassandraClient#getCassandraClient()
     */
    @Override
    public final Cassandra.Client getCassandraClient() throws Exception
    {
        return Pelops.getDbConnPool(POOL_NAME).getConnection().getAPI();
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
    public void setKeySpace(String keySpace)
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
     * Gets the ec cache handler.
     * 
     * @return the scCacheHandler
     */
    public EmbeddedCollectionCacheHandler getEcCacheHandler()
    {
        return ecCacheHandler;
    }

    /**
     * The Class SolandraUtils.
     */
    private class SolandraUtils
    {

        /**
         * Start solandra server.
         */
        public void startSolandraServer()
        {
            log.info("Starting Solandra Server.");
            new CassandraUtils();
            CassandraUtils.cacheInvalidationInterval = 0; // real-time

            try
            {

                createCassSchema();
                Thread.sleep(10000);
//                CassandraUtils.startupServer();
            }
            catch (Throwable t)
            {
                log.error("errror while starting solandra schema:", t);
            }

        }

        /**
         * Creates the cass schema.
         * 
         * @throws IOException
         *             Signals that an I/O exception has occurred.
         */
        private void createCassSchema() throws IOException
        {

            final String keySpace = "L";
            final String termVecColumnFamily = "TI";
            final String docColumnFamily = "Docs";
            final String metaInfoColumnFamily = "TL";
            final String fieldCacheColumnFamily = "FC";

            final String schemaInfoColumnFamily = "SI";

            if (DatabaseDescriptor.getNonSystemTables().contains(keySpace))
            {
                log.info("Found Solandra specific schema");
                return;
            }

            try
            {
                Thread.sleep(1000);

                int sleep = new Random().nextInt(6000);

                log.info("\nSleeping " + sleep + "ms to stagger solandra schema creation\n");

                Thread.sleep(sleep);
            }
            catch (InterruptedException e1)
            {
                e1.printStackTrace();
                System.exit(2);
            }

            if (DatabaseDescriptor.getNonSystemTables().contains(keySpace))
            {
                log.info("Found Solandra specific schema");
                return;
            }

            List<CfDef> cfs = new ArrayList<CfDef>();

            CfDef cf = new CfDef();
            cf.setName(docColumnFamily);
            cf.setComparator_type("BytesType");
            cf.setKey_cache_size(0);
            cf.setRow_cache_size(0);
            cf.setComment("Stores the document and field data for each doc with docId as key");
            cf.setKeyspace(keySpace);

            cfs.add(cf);

            cf = new CfDef();
            cf.setName(termVecColumnFamily);
            cf.setComparator_type("lucandra.VIntType");
            cf.setKey_cache_size(0);
            cf.setRow_cache_size(0);
            cf.setComment("Stores term information with indexName/field/term as composite key");
            cf.setKeyspace(keySpace);

            cfs.add(cf);

            cf = new CfDef();
            cf.setName(fieldCacheColumnFamily);
            cf.setComparator_type("lucandra.VIntType");
            cf.setKey_cache_size(0);
            cf.setRow_cache_size(0);
            cf.setComment("Stores term per doc per field");
            cf.setKeyspace(keySpace);

            cfs.add(cf);

            cf = new CfDef();
            cf.setName(metaInfoColumnFamily);
            cf.setComparator_type("BytesType");
            cf.setKey_cache_size(0);
            cf.setRow_cache_size(0);
            cf.setComment("Stores ordered list of terms for a given field with indexName/field as composite key");
            cf.setKeyspace(keySpace);

            cfs.add(cf);

            cf = new CfDef();
            cf.setName(schemaInfoColumnFamily);
            cf.setColumn_type("Super");
            cf.setComparator_type("BytesType");
            cf.setKey_cache_size(0);
            cf.setRow_cache_size(0);
            cf.setComment("Stores solr and index id information");
            cf.setKeyspace(keySpace);

            cfs.add(cf);

            Class<? extends AbstractReplicationStrategy> simple = SimpleStrategy.class;
            KsDef solandraKS = new KsDef(keySpace, simple.getCanonicalName(), 1, cfs);
            Cassandra.Client client = getClient();

            try
            {
                client.send_system_add_keyspace(solandraKS);
            }
            catch (TException e)
            {
                throw new IOException(e);
            }
            catch (Exception e)
            {
                throw new IOException(e);
            }

            log.info("Added Solandra specific schema");
        }

        /**
         * Inits the client.
         * 
         * @return the client
         */
        private Cassandra.Client getClient()
        {
            TSocket socket = new TSocket(contactNodes[0], defaultPort);
            TTransport transport = new TFramedTransport(socket);
            TProtocol protocol = new TBinaryProtocol(transport);
            Cassandra.Client client = new Cassandra.Client(protocol);

            try
            {
                if (!socket.isOpen())
                {
                    socket.open();

                }
            }
            catch (TTransportException ttex)
            {
                log.error(ttex.getMessage());
            }
            catch (Exception ex)
            {
                log.error(ex.getMessage());
            }
            return client;

        }
    }

}
