/**
 * 
 */
package com.impetus.client.redis;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.loader.GenericClientFactory;

/**
 * @author vivek
 *
 */
public class RedisClientFactory extends GenericClientFactory
{

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.ClientFactory#getSchemaManager()
     */
    @Override
    public SchemaManager getSchemaManager()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.ClientLifeCycleManager#destroy()
     */
    @Override
    public void destroy()
    {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.GenericClientFactory#initialize()
     */
    @Override
    public void initialize()
    {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.GenericClientFactory#createPoolOrConnection()
     */
    @Override
    protected Object createPoolOrConnection()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.GenericClientFactory#instantiateClient(java.lang.String)
     */
    @Override
    protected Client instantiateClient(String persistenceUnit)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.GenericClientFactory#isThreadSafe()
     */
    @Override
    public boolean isThreadSafe()
    {
        // TODO Auto-generated method stub
        return false;
    }

}
