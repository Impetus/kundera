package com.impetus.kundera.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.query.CoreTestEntityReader;
import com.impetus.kundera.query.LuceneQuery;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * The Class CoreTestClient.
 */
public class CoreTestClient extends ClientBase implements Client<LuceneQuery>, ClientPropertiesSetter
{

    // private static int idCount;

    /**
     * Instantiates a new core test client.
     * 
     * @param indexManager
     *            the index manager
     * @param persistenceUnit
     *            the persistence unit
     * @param kunderaMetadata
     *            the kundera metadata
     */
    public CoreTestClient(IndexManager indexManager, String persistenceUnit, final KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata, null, persistenceUnit);
        this.indexManager = indexManager;

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.ClientBase#onPersist(com.impetus.kundera.metadata
     * .model.EntityMetadata, java.lang.Object, java.lang.Object,
     * java.util.List)
     */
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.Object)
     */
    @Override
    public Object find(Class entityClass, Object key)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);
        DummySchema schema = DummyDatabase.INSTANCE.getSchema(m.getSchema());
        if (schema == null)
            return null;

        DummyTable table = schema.getTable(m.getTableName());

        if (table == null)
            return null;

        return table.getRecord(key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#delete(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    public void delete(Object entity, Object pKey)
    {
        if (entity == null)
            return;
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        DummySchema schema = DummyDatabase.INSTANCE.getSchema(m.getSchema());
        if (schema == null)
            return;
        DummyTable table = schema.getTable(m.getTableName());
        if (table == null)
            return;

        table.removeRecord(pKey);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class,
     * java.lang.String[], java.lang.Object[])
     */
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

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {

        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close()
    {

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientBase#getPersistenceUnit()
     */
    @Override
    public String getPersistenceUnit()
    {
        return persistenceUnit;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#persistJoinTable(com.impetus.kundera
     * .persistence.context.jointable.JoinTableData)
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        /*
         * DummySchema schema =
         * DummyDatabase.INSTANCE.getSchema(joinTableData.getSchemaName()); if
         * (schema == null) { schema = new DummySchema();
         * schema.addTable(joinTableData.getJoinTableName(), new DummyTable());
         * }
         * 
         * DummyTable table = schema.getTable(joinTableData.getJoinTableName());
         * Map<Object, Set<Object>> joinTableRecords =
         * joinTableData.getJoinTableRecords();
         * 
         * Iterator iter = joinTableRecords.keySet().iterator();
         * 
         * while (iter.hasNext()) { Object key = iter.next();
         * table.addRecord(key, joinTableRecords.get(key)); }
         */}

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findByRelation(java.lang.String,
     * java.lang.Object, java.lang.Class)
     */
    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {

        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getReader()
     */
    @Override
    public EntityReader getReader()
    {
        return new CoreTestEntityReader(kunderaMetadata);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getQueryImplementor()
     */
    @Override
    public Class<LuceneQuery> getQueryImplementor()
    {
        return LuceneQuery.class;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getColumnsById(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue, Class columnJavaType)
    {

        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findIdsByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {

        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#deleteByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.Object)
     */
    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {

    }

    /** The core test property. */
    String coreTestProperty;

    /**
     * Gets the core test property.
     * 
     * @return the coreTestProperty
     */
    public String getCoreTestProperty()
    {
        return coreTestProperty;
    }

    /**
     * Sets the core test property.
     * 
     * @param coreTestProperty
     *            the coreTestProperty to set
     */
    public void setCoreTestProperty(String coreTestProperty)
    {
        this.coreTestProperty = coreTestProperty;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.ClientPropertiesSetter#populateClientProperties
     * (com.impetus.kundera.client.Client, java.util.Map)
     */
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

    /**
     * Sets the index manager.
     * 
     * @param im
     *            the new index manager
     */
    public void setIndexManager(IndexManager im)
    {
        this.indexManager = im;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIdGenerator()
     */
    @Override
    public Generator getIdGenerator()
    {
        return (Generator) KunderaCoreUtils.createNewInstance(CoreTestIdGenerator.class);
    }

}
