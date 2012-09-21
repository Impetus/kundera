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
    public SchemaManager getSchemaManager()
    {
        return null;
    }

    @Override
    public void load(String persistenceUnit)
    {
        super.load(persistenceUnit);
    }

    @Override
    protected void loadClientMetadata()
    {
        super.loadClientMetadata();
    }

    @Override
    public void initialize()
    {
    }

    @Override
    protected Object createPoolOrConnection()
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
        return null;
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

}
