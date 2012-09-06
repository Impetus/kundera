package com.impetus.kundera.cache.ehcache;

import java.util.List;
import java.util.Map;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.query.LuceneQuery;

public class CoreTestClient implements Client<LuceneQuery>
{

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
    public <E> List<E> getColumnsById(String tableName, String pKeyColumnName, String columnName, Object pKeyColumnValue)
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
    public void deleteByColumn(String tableName, String columnName, Object columnValue)
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

}
