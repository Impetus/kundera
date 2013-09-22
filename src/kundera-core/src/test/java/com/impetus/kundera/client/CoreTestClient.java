package com.impetus.kundera.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.generator.IdentityGenerator;
import com.impetus.kundera.generator.SequenceGenerator;
import com.impetus.kundera.generator.TableGenerator;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.SequenceGeneratorDiscriptor;
import com.impetus.kundera.metadata.model.TableGeneratorDiscriptor;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.query.CoreTestEntityReader;
import com.impetus.kundera.query.LuceneQuery;

public class CoreTestClient extends ClientBase implements Client<LuceneQuery>, AutoGenerator, TableGenerator,
        SequenceGenerator, IdentityGenerator, ClientPropertiesSetter
{

    private static int idCount;

    public CoreTestClient(IndexManager indexManager, String persistenceUnit)
    {
        this.indexManager = indexManager;
        this.persistenceUnit = persistenceUnit;
    }

    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        DummySchema schema = DummyDatabase.INSTANCE.getSchema(entityMetadata.getSchema());

        if (schema == null)
        {
            schema = new DummySchema();
            DummyTable table = new DummyTable();
            table.addRecord(id, entity);
            schema.addTable(entityMetadata.getTableName(), table);
            DummyDatabase.INSTANCE.addSchema(entityMetadata.getSchema(), schema);
        }
        else
        {
            DummyTable table = schema.getTable(entityMetadata.getTableName());
            if (table == null)
            {
                table = new DummyTable();
            }
            table.addRecord(id, entity);
            schema.addTable(entityMetadata.getTableName(), table);
        }
        
       
    }

    @Override
    public Object find(Class entityClass, Object key)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entityClass);
        DummySchema schema = DummyDatabase.INSTANCE.getSchema(m.getSchema());
        if (schema == null)
            return null;

        DummyTable table = schema.getTable(m.getTableName());

        if (table == null)
            return null;

        return table.getRecord(key);
    }

    @Override
    public void delete(Object entity, Object pKey)
    {
        if (entity == null)
            return;
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entity.getClass());
        DummySchema schema = DummyDatabase.INSTANCE.getSchema(m.getSchema());
        if (schema == null)
            return;
        DummyTable table = schema.getTable(m.getTableName());
        if (table == null)
            return;

        table.removeRecord(pKey);
    }

    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys)
    {
        List results = new ArrayList();
        for (Object key : keys)
        {
            Object result = find(entityClass, key);
            if (result != null)
            {
                results.add(result);
            }
        }
        return results;
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
/*        DummySchema schema = DummyDatabase.INSTANCE.getSchema(joinTableData.getSchemaName());
        if (schema == null)
        {
            schema = new DummySchema();
            schema.addTable(joinTableData.getJoinTableName(), new DummyTable());
        }

        DummyTable table = schema.getTable(joinTableData.getJoinTableName());
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        Iterator iter = joinTableRecords.keySet().iterator();

        while (iter.hasNext())
        {
            Object key = iter.next();
            table.addRecord(key, joinTableRecords.get(key));
        }
*/    }

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

    @Override
    public Object generate(SequenceGeneratorDiscriptor discriptor)
    {
        // TODO Auto-generated method stub
        return ++idCount;
    }

    @Override
    public Object generate(TableGeneratorDiscriptor discriptor)
    {
        return ++idCount;
    }

    @Override
    public Object generate()
    {
        return ++idCount;
    }
    
    String coreTestProperty;

    /**
     * @return the coreTestProperty
     */
    public String getCoreTestProperty()
    {
        return coreTestProperty;
    }

    /**
     * @param coreTestProperty the coreTestProperty to set
     */
    public void setCoreTestProperty(String coreTestProperty)
    {
        this.coreTestProperty = coreTestProperty;
    }

    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties)
    {
        if (properties != null)
        {
            for (String key : properties.keySet())
            {
                Object value = properties.get(key);
                if (key.equals("core.test.property") && value instanceof String)
                {
                    setCoreTestProperty((String) value);
                }               
            }
        }        
    }  
    
    public void setIndexManager(IndexManager im){
        this.indexManager = im;
    }
    
    

}
