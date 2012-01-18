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
package com.impetus.client.mongodb.query;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.mongodb.MongoDBClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.QueryImpl;
import com.impetus.kundera.query.exception.QueryHandlerException;

/**
 * Query class for MongoDB data store
 * 
 * @author amresh.singh
 */
public class MongoDBQuery extends QueryImpl
{
    /** The log used by this class. */
    private static Log log = LogFactory.getLog(MongoDBQuery.class);

    public MongoDBQuery(String jpaQuery, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            String... persistenceUnits)
    {
        super(jpaQuery, persistenceDelegator, persistenceUnits);
        this.kunderaQuery = kunderaQuery;
    }

    
    @Override
    public int executeUpdate()
    {
        return super.executeUpdate();
    }

    @Override
    public Query setMaxResults(int maxResult)
    {
        return super.setMaxResults(maxResult);
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.query.QueryImpl#populateEntities(com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    

    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        //TODO : Must refactor client 
        try
        {
            return ((MongoDBClient) client).loadData(m, getKunderaQuery(), null);
        }
        catch (Exception e)
        {
            throw new QueryHandlerException(e.getMessage());
        }
        
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.query.QueryImpl#handleAssociations(com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.client.Client, java.util.List, java.util.List, boolean)
     */
    @Override
    protected List<Object> handleAssociations(EntityMetadata m, Client client, List<EntitySaveGraph> graphs,
            List<String> relationNames, boolean isParent)
    {
        //TODO : required to modify client return relation.
        // if it is a parent..then find data related to it only
        // else u need to load for associated fields too.
        List<EnhanceEntity> ls = new ArrayList<EnhanceEntity>();

            try
            {
                ls = ((MongoDBClient) client).loadData(m, kunderaQuery, relationNames);
            }
            catch (Exception e)
            {
                throw new QueryHandlerException(e.getMessage());
            }
        return handleGraph(ls, graphs,client, m);
        
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        return null;
    }

    
}
