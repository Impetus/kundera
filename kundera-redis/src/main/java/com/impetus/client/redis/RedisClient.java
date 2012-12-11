/**
 * 
 */
package com.impetus.client.redis;

import java.util.List;
import java.util.Map;

import com.impetus.client.redis.query.RedisQuery;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.api.Batcher;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;

/**
 * @author vivek.mishra
 *
 */
public class RedisClient extends ClientBase implements Client<RedisQuery>, Batcher
{

    /* (non-Javadoc)
     * @see com.impetus.kundera.persistence.api.Batcher#addBatch(com.impetus.kundera.graph.Node)
     */
    @Override
    public void addBatch(Node node)
    {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.persistence.api.Batcher#executeBatch()
     */
    @Override
    public int executeBatch()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.persistence.api.Batcher#getBatchSize()
     */
    @Override
    public int getBatchSize()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.ClientBase#onPersist(com.impetus.kundera.metadata.model.EntityMetadata, java.lang.Object, java.lang.Object, java.util.List)
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public Object find(Class entityClass, Object key)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <E> List<E> findAll(Class<E> entityClass, Object... keys)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
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
    public void persistJoinTable(JoinTableData joinTableData)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public <E> List<E> getColumnsById(String tableName, String pKeyColumnName, String columnName, Object pKeyColumnValue)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object[] findIdsByColumn(String tableName, String pKeyName, String columnName, Object columnValue,
            Class entityClazz)
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
    public void deleteByColumn(String tableName, String columnName, Object columnValue)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
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
    public Class<RedisQuery> getQueryImplementor()
    {
        return RedisQuery.class;
    }

}
