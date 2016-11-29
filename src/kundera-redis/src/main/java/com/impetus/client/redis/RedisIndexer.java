/**
 * Copyright 2013 Impetus Infotech.
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

package com.impetus.client.redis;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import com.impetus.client.redis.RedisClient.AttributeWrapper;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.index.Indexer;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;

/**
 * @author vivek.mishra
 * 
 */
public class RedisIndexer implements Indexer
{

    private Object pipeLineOrConnection;

    private static Logger logger = LoggerFactory.getLogger(RedisIndexer.class);

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.index.Indexer#index(java.lang.Class,
     * java.util.Map, java.lang.Object, java.lang.Class)
     */
    @Override
    public void index(Class entityClazz, EntityMetadata m, Map<String, Object> values, Object parentId,
            Class parentClazz)
    {
        Set<String> indexNames = values.keySet();
        for (String idx_Name : indexNames)
        {
            Double value = (Double) values.get(idx_Name);
            Pipeline pipeLine = null;
            try
            {
                if (this.pipeLineOrConnection.getClass().isAssignableFrom(Jedis.class))
                {
                    pipeLine = ((Jedis) this.pipeLineOrConnection).pipelined();
                    pipeLine.zadd(idx_Name, value, parentId.toString());
                    // pipeLine.sync();
                }
                else
                {
                    ((Transaction) this.pipeLineOrConnection).zadd(idx_Name, value, parentId.toString());
                }
            }
            finally
            {
                if (pipeLine != null)
                {
                    pipeLine.sync();
                }
            }
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.index.Indexer#search(java.lang.Class,
     * java.lang.String, int, int)
     */
    @Override
    public Map<String, Object> search(Class<?> clazz, EntityMetadata m, String queryString, int start, int count)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.index.Indexer#search(java.lang.String,
     * java.lang.Class, java.lang.Class, java.lang.Object, int, int)
     */
    @Override
    public Map<String, Object> search(String query, Class<?> parentClass, EntityMetadata parentMetadata,
            Class<?> childClass, EntityMetadata childMetadata, Object entityId, int start, int count)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.index.Indexer#unIndex(java.lang.Class,
     * java.lang.Object)
     */
    @Override
    public void unIndex(Class entityClazz, Object entity, EntityMetadata metadata, MetamodelImpl metamodel)
    {
        // we need not implement this method for Redis because
        // redis automatically removes indexes while performing delete
        logger.warn("Removing index is implicitly managed by RedisClient's unindex method");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.index.Indexer#close()
     */
    @Override
    public void close()
    {
    }

    void assignConnection(Object connection)
    {
        this.pipeLineOrConnection = connection;
    }

    private void unIndex(final AttributeWrapper wrapper, final String member)
    {
        Set<String> keys = wrapper.getIndexes().keySet();

        // keys
        for (String key : keys)
        {
            if (this.pipeLineOrConnection.getClass().isAssignableFrom(Transaction.class))
            {
                ((Transaction) this.pipeLineOrConnection).zrem(key, member);

            }
            else
            {
                ((Pipeline) this.pipeLineOrConnection).zrem(key, member);

            }
        }
    }

    @Override
    public Map<String, Object> search(KunderaMetadata kunderaMetadata, KunderaQuery kunderaQuery,
            PersistenceDelegator persistenceDelegator, EntityMetadata m, int firstResult, int maxResults)
    {
        throw new KunderaException("Unsupported Method");
    }
}
