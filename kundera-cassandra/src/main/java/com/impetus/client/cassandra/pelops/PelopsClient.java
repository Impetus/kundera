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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.persistence.PersistenceException;
import javax.persistence.Query;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.RowDeletor;
import org.scale7.cassandra.pelops.Selector;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.db.DataRow;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
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

    /** log for this class. */
    private static Log log = LogFactory.getLog(PelopsClient.class);

    /** The closed. */
    private boolean closed = false;

    /** The data handler. */
    private PelopsDataHandler dataHandler;

    /** The index manager. */
    private IndexManager indexManager;

    private String persistenceUnit;

    /**
     * default constructor.
     */
    public PelopsClient(IndexManager indexManager)
    {
        this.indexManager = indexManager;
        this.dataHandler = new PelopsDataHandler(this);
    }

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

        PelopsDataHandler.ThriftRow tf = dataHandler.toThriftRow(this, enhancedEntity, entityMetadata, columnFamily);

        Mutator mutator = Pelops.createMutator(PelopsUtils.generatePoolName(getPersistenceUnit()));

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
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));

        E e = (E) dataHandler.fromThriftRow(selector, entityClass, entityMetadata, rowId.toString());

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
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));

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
            E e = (E) dataHandler.fromThriftRow(entityMetadata.getEntityClazz(), entityMetadata,
                    new DataRow<SuperColumn>(entityId, entityMetadata.getTableName(), superColumnList));
            entities.add(e);
        }
        return entities;
    }

    private final List<SuperColumn> loadSuperColumns(String keyspace, String columnFamily, String rowId,
            String... superColumnNames) throws Exception
    {
        if (!isOpen())
            throw new PersistenceException("PelopsClient is closed.");
        Selector selector = Pelops.createSelector(PelopsUtils.generatePoolName(getPersistenceUnit()));
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

        RowDeletor rowDeletor = Pelops.createRowDeletor(PelopsUtils.generatePoolName(getPersistenceUnit()));
        rowDeletor.deleteRow(entityMetadata.getTableName(), enhancedEntity.getId(), ConsistencyLevel.ONE);
        getIndexManager().remove(entityMetadata, enhancedEntity.getEntity(), enhancedEntity.getId());
    }

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

    private final boolean isOpen()
    {
        return !closed;
    }

    @Override
    public final void close()
    {
        this.indexManager = null;
        this.dataHandler = null;
        closed = true;

    }

    @Override
    public void setPersistenceUnit(String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
    }

}
