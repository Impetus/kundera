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

import java.util.Map;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;

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
        return new CoreTestClient();

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
