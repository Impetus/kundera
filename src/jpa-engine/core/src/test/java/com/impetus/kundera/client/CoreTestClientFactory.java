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
package com.impetus.kundera.client;

import java.util.Map;

import com.impetus.kundera.configure.schema.api.CoreSchemaManager;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;

/**
 * <Prove description of functionality provided by this Type>
 * 
 * @author amresh.singh
 */
public class CoreTestClientFactory extends GenericClientFactory
{

    private  SchemaManager schemaManager;
    
    @Override
    public void destroy()
    {
        schemaManager.dropSchema();
        super.unload();
    }

    @Override
    public SchemaManager getSchemaManager(Map<String, Object> puProperties)
    {
        if(schemaManager == null)
        schemaManager =  new CoreSchemaManager("com.impetus.kundera.client.CoreTestClientFactory", puProperties, kunderaMetadata);
        return schemaManager;
    }

    @Override
    public Client getClientInstance()
    {
        return super.getClientInstance();
    }

    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        setConnectionPoolOrConnection(null);
        
        return new CoreTestClient(indexManager, persistenceUnit, kunderaMetadata);
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    public String getPersistenceUnit()
    {
        return super.getPersistenceUnit();
    }

    public Object getConnectionPoolOrConnection()
    {
        
        return super.getConnectionPoolOrConnection();
    }
  
    public void onValidation(final String host, final String port)
    {
        super.onValidation(host, port);
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

    public LoadBalancer getLoadBalancePolicy(final String  policy)
    {
        return LoadBalancer.getValue(policy);
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

    @Override
    protected void initializeLoadBalancer(String loadBalancingPolicyName)
    {
        throw new UnsupportedOperationException("Load balancing feature is not supported in "
                + this.getClass().getSimpleName());
    }
}
