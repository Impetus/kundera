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
package com.impetus.kundera.hbase.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.kundera.ejb.EntityManagerImpl;
import com.impetus.kundera.hbase.admin.DataHandler;
import com.impetus.kundera.hbase.admin.HBaseDataHandler;
import com.impetus.kundera.loader.DBType;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.EntityMetadata.Column;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.EnhancedEntity;

/**
 * HBase client.
 *
 * @author impetus
 */
public class HBaseClient implements com.impetus.kundera.Client
{

    /** The contact node. */
    String contactNode;

    /** The default port. */
    String defaultPort;

    /** The handler. */
    private DataHandler handler;

    /** The is connected. */
    private boolean isConnected;

    /** The em. */
    private EntityManager em;

    /*
     * (non-Javadoc)
     *
     * @see com.impetus.kundera.Client#writeColumns(java.lang.String,
     * java.lang.String, java.lang.String, java.util.List,
     * com.impetus.kundera.proxy.EnhancedEntity)
     */
    @Override
    public void writeColumns(String keyspace, String columnFamily, String rowKey, List<Column> columns, EnhancedEntity e)
            throws Exception
    {
        handler.loadData(e.getEntity().getClass().getSimpleName().toLowerCase(), columnFamily, rowKey, columns, e);
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
        throw new PersistenceException("Not yet implemented");
    }

    /*
     * (non-Javadoc)
     *
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManagerImpl, java.lang.Class, java.lang.String, java.lang.String,
     * java.lang.String, com.impetus.kundera.metadata.EntityMetadata)
     */
    @Override
    public <E> E loadColumns(EntityManagerImpl em, Class<E> clazz, String keyspace, String columnFamily, String rowKey,
            EntityMetadata m) throws Exception
    {
        HBaseData data = handler.populateData(clazz.getSimpleName().toLowerCase(), columnFamily, new String[0], rowKey);
        return onLoadFromHBase(clazz, data, m, rowKey);
    }

    /*
     * (non-Javadoc)
     *
     * @seecom.impetus.kundera.Client#loadColumns(com.impetus.kundera.ejb.
     * EntityManagerImpl, java.lang.Class, java.lang.String, java.lang.String,
     * com.impetus.kundera.metadata.EntityMetadata, java.lang.String[])
     */
    @Override
    public <E> List<E> loadColumns(EntityManagerImpl em, Class<E> clazz, String keyspace, String columnFamily,
            EntityMetadata m, String... keys) throws Exception
    {
        List<E> entities = new ArrayList<E>();
        for (String rowKey : keys)
        {
            HBaseData data = handler.populateData(clazz.getSimpleName().toLowerCase(), columnFamily, keys, rowKey);
            entities.add(onLoadFromHBase(clazz, data, m, rowKey));
        }
        return entities;
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

    /**
     * On load from h base.
     *
     * @param <E>
     *            the element type
     * @param clazz
     *            the clazz
     * @param data
     *            the data
     * @param m
     *            the m
     * @param id
     *            the id
     * @return the e
     */
    private <E> E onLoadFromHBase(Class<E> clazz, HBaseData data, EntityMetadata m, String id)
    {
        // Instantiate a new instance
        E e = null;
        try
        {
            e = clazz.newInstance();
            String colName = null;
            byte[] columnValue = null;
            PropertyAccessorHelper.set(e, m.getIdProperty(), id);
            List<KeyValue> values = data.getColumns();
            for (KeyValue colData : values)
            {
                colName = Bytes.toString(colData.getQualifier());
                columnValue = colData.getValue();

                // Get Column from metadata
                com.impetus.kundera.metadata.EntityMetadata.Column column = m.getColumn(colName);
                PropertyAccessorHelper.set(e, column.getField(), columnValue);
            }
        }
        catch (InstantiationException e1)
        {
            throw new RuntimeException(e1.getMessage());
        }
        catch (IllegalAccessException e1)
        {
            throw new RuntimeException(e1.getMessage());
        }
        catch (PropertyAccessException e1)
        {
            throw new RuntimeException(e1.getMessage());
        }

        return e;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.impetus.kundera.Client#shutdown()
     */
    @Override
    public void shutdown()
    {
        handler.shutdown();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.impetus.kundera.Client#connect()
     */
    @Override
    public void connect()
    {
        if (!isConnected)
        {
            handler = new HBaseDataHandler(contactNode, defaultPort);
            isConnected = true;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.impetus.kundera.Client#setContactNodes(java.lang.String[])
     */
    @Override
    public void setContactNodes(String... contactNodes)
    {
        this.contactNode = contactNodes[0];
    }

    /*
     * (non-Javadoc)
     *
     * @see com.impetus.kundera.Client#setDefaultPort(int)
     */
    @Override
    public void setDefaultPort(int defaultPort)
    {
        this.defaultPort = String.valueOf(defaultPort);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.impetus.kundera.Client#delete(java.lang.String,
     * java.lang.String, java.lang.String)
     */
    @Override
    public void delete(String keyspace, String columnFamily, String rowId) throws Exception
    {
        throw new RuntimeException("TODO:not yet supprot");

    }

    /*
     * (non-Javadoc)
     *
     * @see com.impetus.kundera.Client#setKeySpace(java.lang.String)
     */
    @Override
    public void setKeySpace(String keySpace)
    {
        // TODO not required.
    }

    /*
     * (non-Javadoc)
     *
     * @see com.impetus.kundera.Client#getType()
     */
    @Override
    public DBType getType()
    {
        return DBType.HBASE;
    }
}
