/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.cassandra.thrift;

import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.cassandra.datahandler.DataHandler;
import com.impetus.client.cassandra.query.CassQuery;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;

/**
 * Kundera Client implementation for Cassandra using Thrift library 
 * @author amresh.singh
 */
public class ThriftClient extends ClientBase implements Client<CassQuery>
{
    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

    /** log for this class. */
    private static Log log = LogFactory.getLog(ThriftClient.class);

    /** The closed. */
    private boolean closed = false;

    /** The data handler. */
    private DataHandler handler;

    /** The reader. */
    private EntityReader reader;

    /** The timestamp. */
    private long timestamp;
    
    public ThriftClient(IndexManager indexManager, EntityReader reader, String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
        this.indexManager = indexManager;
        this.handler = new ThriftDataHandler();
        this.reader = reader;
    }

    @Override
    public Object find(Class entityClass, Object key)
    {
        return null;
    }

    @Override
    public <E> List<E> findAll(Class<E> entityClass, Object... keys)
    {
        return null;
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        return null;
    }

    @Override
    public void close()
    {
    }

    @Override
    public void delete(Object entity, Object pKey)
    {
    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
    }

    @Override
    public <E> List<E> getColumnsById(String tableName, String pKeyColumnName, String columnName, String pKeyColumnValue)
    {
        return null;
    }

    @Override
    public Object[] findIdsByColumn(String tableName, String pKeyName, String columnName, Object columnValue,
            Class entityClazz)
    {
        return null;
    }

    @Override
    public void deleteByColumn(String tableName, String columnName, Object columnValue)
    {
    }

    @Override
    public List<Object> findByRelation(String colName, String colValue, Class entityClazz)
    {
        return null;
    }

    @Override
    public EntityReader getReader()
    {
        return null;
    }

    @Override
    public Class<CassQuery> getQueryImplementor()
    {
        return null;
    }

    @Override
    public String getPersistenceUnit()
    {
        return super.getPersistenceUnit();
    }

    @Override
    public void persist(Node node)
    {
        super.persist(node);
    }

    @Override
    protected List<RelationHolder> getRelationHolders(Node node)
    {
        return super.getRelationHolders(node);
    }

    @Override
    protected void indexNode(Node node, EntityMetadata entityMetadata)
    {
        super.indexNode(node, entityMetadata);
    }

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
    }  

}
