package com.impetus.kundera.cache.ehcache;

import java.util.List;
import java.util.Map;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.query.LuceneQuery;

public class CoreTestClient implements Client<LuceneQuery>
{

    
    
    @Override
    public void persist(Object entity, Object id, List<RelationHolder> rlHolders)
    {
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
    public void persistJoinTable(JoinTableData joinTableData)
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
    public Class<LuceneQuery> getQueryImplementor()
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#getColumnsById(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.Object)
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#findIdsByColumn(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.Object, java.lang.Class)
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.client.Client#deleteByColumn(java.lang.String, java.lang.String, java.lang.String, java.lang.Object)
     */
    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        // TODO Auto-generated method stub
        
    }

}
