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
package com.impetus.client.mongodb.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.index.Indexer;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.PropertyIndex;

/**
 * Provides indexing functionality for MongoDB database.
 * 
 * @author amresh.singh
 */
public class MongoDBIndexer implements Indexer
{
    /** log for this class. */
    private static Log LOG = LogFactory.getLog(MongoDBIndexer.class);

    /** The client. */
    MongoDBClient client;

    /**
     * Instantiates a new mongo db indexer.
     * 
     * @param client
     *            the client
     */
    public MongoDBIndexer(Client client)
    {
        this.client = (MongoDBClient) client;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.index.Indexer#unindex(com.impetus.kundera.metadata
     * .EntityMetadata, java.lang.String)
     */
    @Override
    public void unindex(EntityMetadata metadata, String id)
    {
        LOG.debug("No need to remove data from Index. It's handled automatically by MongoDB when document is dropped");
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.index.Indexer#index(com.impetus.kundera.metadata.
     * EntityMetadata, java.lang.Object)
     */
    @Override
    public void index(EntityMetadata metadata, Object object)
    {
        if (!metadata.isIndexable())
        {
            return;
        }

        LOG.debug("Indexing @Entity[" + metadata.getEntityClazz().getName() + "] " + object);
        String indexName = metadata.getIndexName(); // Index Name=Collection
        // name, not required

        List<PropertyIndex> indexProperties = metadata.getIndexProperties();

        List<String> columnList = new ArrayList<String>();

        for (PropertyIndex propertyIndex : indexProperties)
        {
            columnList.add(propertyIndex.getName());
        }

        client.createIndex(metadata.getTableName(), columnList, 1); // 1=Ascending

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.index.Indexer#search(java.lang.String, int, int)
     */
    @Override
    public Map<String, String> search(String query, int start, int count, boolean fetchRelation)
    {
        throw new PersistenceException(
                "Invalid method call! When you search on a column, MongoDB will automatically search in index if that exists.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.index.Indexer#close()
     */
    @Override
    public void close()
    {
        client.close();

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.index.Indexer#flush()
     */
    @Override
    public void flush()
    {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.index.Indexer#index(com.impetus.kundera.metadata.
     * model.EntityMetadata, java.lang.Object, java.lang.String,
     * java.lang.Class)
     */
    @Override
    public void index(EntityMetadata metadata, Object object, String parentId, Class<?> clazz)
    {
        // TODO Auto-generated method stub

    }

}
