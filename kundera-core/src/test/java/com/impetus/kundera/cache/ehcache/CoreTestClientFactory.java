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
package com.impetus.kundera.cache.ehcache;

import java.util.List;
import java.util.Map;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.loader.GenericClientFactory;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;

/**
 * <Prove description of functionality provided by this Type>
 * 
 * @author amresh.singh
 */
public class CoreTestClientFactory extends GenericClientFactory
{

    @Override
    public void destroy()
    {
    }

    @Override
    public SchemaManager getSchemaManager(Map<String, Object> puProperties)
    {
        return null;
    }

    @Override
    public Client getClientInstance()
    {
        return super.getClientInstance();
    }

    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        return new  Client()
        {

            @Override
            public Object find(Class entityClass, Object key)
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public List findAll(Class entityClass, Object... keys)
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public List find(Class entityClass, Map embeddedColumnMap)
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public void close()
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void delete(Object entity, Object pKey)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public String getPersistenceUnit()
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public IndexManager getIndexManager()
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public void persist(Node node)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void persistJoinTable(JoinTableData joinTableData)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public List getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
                    Object pKeyColumnValue)
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
                    Object columnValue, Class entityClazz)
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
            {
                // TODO Auto-generated method stub
                
            }

            @Override
            public List findByRelation(String colName, Object colValue, Class entityClazz)
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public EntityReader getReader()
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Class getQueryImplementor()
            {
                // TODO Auto-generated method stub
                return null;
            }
        };
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    @Override
    protected String getPersistenceUnit()
    {
        return super.getPersistenceUnit();
    }

    @Override
    protected Object getConnectionPoolOrConnection()
    {
        return super.getConnectionPoolOrConnection();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.loader.ClientFactory#load(java.lang.String,
     * java.util.Map)
     */
    @Override
    public void load(String persistenceUnit, Map<String, Object> puProperties)
    {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#initialize(java.util.Map)
     */
    @Override
    public void initialize(Map<String, Object> puProperties)
    {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.loader.GenericClientFactory#createPoolOrConnection
     * (java.util.Map)
     */
    @Override
    protected Object createPoolOrConnection()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
