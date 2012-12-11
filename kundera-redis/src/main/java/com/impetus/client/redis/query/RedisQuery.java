/**
 * 
 */
package com.impetus.client.redis.query;

import java.util.List;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.QueryImpl;

/**
 * @author vivek
 *
 */
public class RedisQuery extends QueryImpl
{

    public RedisQuery(String query, PersistenceDelegator persistenceDelegator)
    {
        super(query, persistenceDelegator);
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.query.QueryImpl#populateEntities(com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.query.QueryImpl#recursivelyPopulateEntities(com.impetus.kundera.metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.query.QueryImpl#onExecuteUpdate()
     */
    @Override
    protected int onExecuteUpdate()
    {
        // TODO Auto-generated method stub
        return 0;
    }

}
