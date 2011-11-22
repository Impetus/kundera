/**
 * 
 */
package com.impetus.client.rdbms;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.loader.GenericClientFactory;

/**
 * @author impadmin
 *
 */
public class RDBMSClientFactory extends GenericClientFactory
{

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.Loader#unload(java.lang.String[])
     */
    @Override
    public void unload(String... paramArrayOfString)
    {

    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.GenericClientFactory#initializeClient()
     */
    @Override
    protected void initializeClient()
    {
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
     * @see com.impetus.kundera.loader.GenericClientFactory#instantiateClient()
     */
    @Override
    protected Client instantiateClient()
    {
        // TODO Auto-generated method stub
        return new HibernateClient(getPersistenceUnit());
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.loader.GenericClientFactory#isClientThreadSafe()
     */
    @Override
    protected boolean isClientThreadSafe()
    {
        return true;
    }

}
