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
package com.impetus.client.neo4j.query;

import java.util.List;

import javax.persistence.Query;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.QueryImpl;

/**
 * <Prove description of functionality provided by this Type> 
 * @author amresh.singh
 */
public class Neo4JQuery extends QueryImpl implements Query
{
    /**
     * @param query
     * @param persistenceDelegator
     */
    public Neo4JQuery(String query, PersistenceDelegator persistenceDelegator)
    {
        super(query, persistenceDelegator);
        
    }

    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        return null;
    }

    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        return null;
    }

    @Override
    protected EntityReader getReader()
    {
        return null;
    }

    @Override
    protected int onExecuteUpdate()
    {
        return 0;
    }
    
}
