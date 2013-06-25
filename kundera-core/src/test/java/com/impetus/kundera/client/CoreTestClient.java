package com.impetus.kundera.client;

import java.util.List;
import java.util.Map;

import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.query.CoreTestEntityReader;
import com.impetus.kundera.query.LuceneQuery;

public class CoreTestClient extends ClientBase implements Client<LuceneQuery>
{

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        DummySchema schema = DummyDatabase.INSTANCE.getSchema(entityMetadata.getSchema());        
        DummyTable table = new DummyTable();
        table.addRecord(id, entity);
        
        if(schema == null)
        {
            schema = new DummySchema();
            schema.addTable(entityMetadata.getTableName(), table);
            DummyDatabase.INSTANCE.addSchema(entityMetadata.getSchema(), schema);
        }
        else
        {
            schema.addTable(entityMetadata.getTableName(), table);            
        }         
    }
    
    @Override
    public Object find(Class entityClass, Object key)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entityClass);
        DummySchema schema = DummyDatabase.INSTANCE.getSchema(m.getSchema());
        if(schema == null) return null;
        
        DummyTable table = schema.getTable(m.getTableName());
        
        if(table == null) return null;
        
        return table.getRecord(key);        
    }
    
    @Override
    public void delete(Object entity, Object pKey)
    {
        if(entity == null) return;
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entity.getClass());
        DummySchema schema = DummyDatabase.INSTANCE.getSchema(m.getSchema());
        if(schema == null) return;
        DummyTable table = schema.getTable(m.getTableName());
        if(table == null) return;
        
        table.removeRecord(pKey);       
    }
    

    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys)
    {

        return null;
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {

        return null;
    }

    @Override
    public void close()
    {

    }

    

    @Override
    public String getPersistenceUnit()
    {
        return persistenceUnit;
    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {


    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {

        return null;
    }

    @Override
    public EntityReader getReader()
    {
        return new CoreTestEntityReader();
    }

    @Override
    public Class<LuceneQuery> getQueryImplementor()
    {
        return LuceneQuery.class;
    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue, Class columnJavaType)
    {

        return null;
    }


    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {

        return null;
    }


    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {

    }

    

}
