/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.client.kudu;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Embeddable;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.kudu.query.KuduDBQuery;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ClientMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * The Class KuduDBClient.
 * 
 * @author karthikp.manchala
 */
public class KuduDBClient extends ClientBase implements Client<KuduDBQuery>, ClientPropertiesSetter
{

    /** The logger. */
    private static Logger logger = LoggerFactory.getLogger(KuduDBClient.class);

    /** The kudu client. */
    private KuduClient kuduClient;

    /** The reader. */
    private EntityReader reader;

    /**
     * Instantiates a new kudu db client.
     *
     * @param kunderaMetadata
     *            the kundera metadata
     * @param indexManager
     *            the index manager
     * @param reader
     *            the reader
     * @param properties
     *            the properties
     * @param persistenceUnit
     *            the persistence unit
     * @param kuduClient
     *            the kudu client
     * @param clientMetadata
     *            the client metadata
     */
    protected KuduDBClient(KunderaMetadata kunderaMetadata, IndexManager indexManager, EntityReader reader,
            Map<String, Object> properties, String persistenceUnit, KuduClient kuduClient,
            ClientMetadata clientMetadata)
    {
        super(kunderaMetadata, properties, persistenceUnit);
        this.reader = reader;
        this.kuduClient = kuduClient;
        this.indexManager = indexManager;
        this.clientMetadata = clientMetadata;
    }

    /**
     * Gets the kudu client.
     * 
     * @return the kudu client
     */
    public KuduClient getKuduClient()
    {
        return kuduClient;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientPropertiesSetter#
     * populateClientProperties(com.impetus.kundera.client.Client,
     * java.util.Map)
     */
    /**
     * Populate client properties.
     * 
     * @param client
     *            the client
     * @param properties
     *            the properties
     */
    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties)
    { // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.lang.Object)
     */
    /**
     * Find.
     * 
     * @param entityClass
     *            the entity class
     * @param key
     *            the key
     * @return the object
     */
    @Override
    public Object find(Class entityClass, Object key)
    {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(entityMetadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());

        String idColumnName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getName();

        Field field = (Field) entityType.getAttribute(idColumnName).getJavaMember();

        Type idType = KuduDBValidationClassMapper.getValidTypeForClass(field.getType());

        KuduTable table;
        try
        {
            table = kuduClient.openTable(entityMetadata.getTableName());
        }
        catch (Exception e)
        {
            logger.error("Cannot open table : " + entityMetadata.getTableName(), e);
            throw new KunderaException("Cannot open table : " + entityMetadata.getTableName(), e);
        }

        KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(table);
        KuduScanner scanner = null;

        if (entityType.getAttribute(idColumnName).getJavaType().isAnnotationPresent(Embeddable.class))
        {
            // Composite Id
            EmbeddableType embeddableIdType = metaModel.embeddable(entityType.getAttribute(idColumnName).getJavaType());
            Field[] fields = entityType.getAttribute(idColumnName).getJavaType().getDeclaredFields();

            addPredicatesToScannerBuilder(scannerBuilder, embeddableIdType, fields, metaModel, key);
        }
        else
        {
            // Simple Id
            ColumnSchema column = new ColumnSchema.ColumnSchemaBuilder(((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName(), idType).build();

            KuduPredicate predicate = KuduDBDataHandler.getEqualComparisonPredicate(column, idType, key);
            scannerBuilder.addPredicate(predicate);
        }

        scanner = scannerBuilder.build();

        Object entity = null;
        while (scanner.hasMoreRows())
        {
            RowResultIterator results;
            try
            {
                results = scanner.nextRows();
            }
            catch (Exception e)
            {
                logger.error("Cannot get results from table : " + entityMetadata.getTableName(), e);
                throw new KunderaException("Cannot get results from table : " + entityMetadata.getTableName(), e);
            }

            while (results.hasNext())
            {
                RowResult result = results.next();
                entity = KunderaCoreUtils.createNewInstance(entityClass);
                populateEntity(entity, result, entityType, metaModel);
                logger.debug(result.rowToString());
            }
        }
        return entity;
    }

    /**
     * Adds the predicates to scanner builder.
     *
     * @param scannerBuilder
     *            the scanner builder
     * @param embeddable
     *            the embeddable
     * @param fields
     *            the fields
     * @param metaModel
     *            the meta model
     * @param key
     *            the key
     */
    private void addPredicatesToScannerBuilder(KuduScannerBuilder scannerBuilder, EmbeddableType embeddable,
            Field[] fields, MetamodelImpl metaModel, Object key)
    {
        for (Field f : fields)
        {
            if (!ReflectUtils.isTransientOrStatic(f))
            {
                Object value = PropertyAccessorHelper.getObject(key, f);
                if (f.getType().isAnnotationPresent(Embeddable.class))
                {
                    // nested
                    addPredicatesToScannerBuilder(scannerBuilder, (EmbeddableType) metaModel.embeddable(f.getType()),
                            f.getType().getDeclaredFields(), metaModel, value);
                }
                else
                {

                    Attribute attribute = embeddable.getAttribute(f.getName());
                    Type type = KuduDBValidationClassMapper.getValidTypeForClass(f.getType());
                    ColumnSchema column = new ColumnSchema.ColumnSchemaBuilder(
                            ((AbstractAttribute) attribute).getJPAColumnName(), type).build();
                    KuduPredicate predicate = KuduDBDataHandler.getEqualComparisonPredicate(column, type, value);
                    scannerBuilder.addPredicate(predicate);
                }
            }
        }

    }

    /**
     * Populate entity.
     * 
     * @param entity
     *            the entity
     * @param result
     *            the result
     * @param entityType
     *            the entity type
     * @param metaModel
     *            the meta model
     */
    public void populateEntity(Object entity, RowResult result, EntityType entityType, MetamodelImpl metaModel)
    {
        Set<Attribute> attributes = entityType.getAttributes();
        Iterator<Attribute> iterator = attributes.iterator();
        iterateAndPopulateEntity(entity, result, metaModel, iterator);
    }

    /**
     * Populate embedded column.
     * 
     * @param entity
     *            the entity
     * @param result
     *            the result
     * @param embeddable
     *            the embeddable
     * @param metaModel
     *            the meta model
     */
    private void populateEmbeddedColumn(Object entity, RowResult result, EmbeddableType embeddable,
            MetamodelImpl metaModel)
    {
        Set<Attribute> attributes = embeddable.getAttributes();
        Iterator<Attribute> iterator = attributes.iterator();

        iterateAndPopulateEntity(entity, result, metaModel, iterator);
    }

    /**
     * Iterate and populate entity.
     * 
     * @param entity
     *            the entity
     * @param result
     *            the result
     * @param metaModel
     *            the meta model
     * @param iterator
     *            the iterator
     */
    private void iterateAndPopulateEntity(Object entity, RowResult result, MetamodelImpl metaModel,
            Iterator<Attribute> iterator)
    {
        while (iterator.hasNext())
        {
            Attribute attribute = iterator.next();
            Field field = (Field) attribute.getJavaMember();
            // handle for embeddables
            if (attribute.getJavaType().isAnnotationPresent(Embeddable.class))
            {
                EmbeddableType emb = metaModel.embeddable(attribute.getJavaType());
                Object embeddableObj = KunderaCoreUtils.createNewInstance(attribute.getJavaType());
                populateEmbeddedColumn(embeddableObj, result, emb, metaModel);
                PropertyAccessorHelper.set(entity, field, embeddableObj);
            }
            else
            {
                if (KuduDBDataHandler.hasColumn(result.getSchema(), ((AbstractAttribute) attribute).getJPAColumnName()))
                {
                    PropertyAccessorHelper.set(entity, field, KuduDBDataHandler.getColumnValue(result,
                            ((AbstractAttribute) attribute).getJPAColumnName()));
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class,
     * java.lang.String[], java.lang.Object[])
     */
    /**
     * Find all.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param columnsToSelect
     *            the columns to select
     * @param keys
     *            the keys
     * @return the list
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys)
    {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class,
     * java.util.Map)
     */
    /**
     * Find.
     * 
     * @param <E>
     *            the element type
     * @param entityClass
     *            the entity class
     * @param embeddedColumnMap
     *            the embedded column map
     * @return the list
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    { // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    /**
     * Close.
     */
    @Override
    public void close()
    {// TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#persistJoinTable(com.impetus.kundera.
     * persistence.context.jointable.JoinTableData )
     */
    /**
     * Persist join table.
     * 
     * @param joinTableData
     *            the join table data
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {// TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getColumnsById(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
    /**
     * Gets the columns by id.
     * 
     * @param <E>
     *            the element type
     * @param schemaName
     *            the schema name
     * @param tableName
     *            the table name
     * @param pKeyColumnName
     *            the key column name
     * @param columnName
     *            the column name
     * @param pKeyColumnValue
     *            the key column value
     * @param columnJavaType
     *            the column java type
     * @return the columns by id
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue, Class columnJavaType)
    {// TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findIdsByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.Object,
     * java.lang.Class)
     */
    /**
     * Find ids by column.
     * 
     * @param schemaName
     *            the schema name
     * @param tableName
     *            the table name
     * @param pKeyName
     *            the key name
     * @param columnName
     *            the column name
     * @param columnValue
     *            the column value
     * @param entityClazz
     *            the entity clazz
     * @return the object[]
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {// TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#deleteByColumn(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.Object)
     */
    /**
     * Delete by column.
     * 
     * @param schemaName
     *            the schema name
     * @param tableName
     *            the table name
     * @param columnName
     *            the column name
     * @param columnValue
     *            the column value
     */
    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    { // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findByRelation(java.lang.String,
     * java.lang.Object, java.lang.Class)
     */
    /**
     * Find by relation.
     * 
     * @param colName
     *            the col name
     * @param colValue
     *            the col value
     * @param entityClazz
     *            the entity clazz
     * @return the list
     */
    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {// TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getReader()
     */
    /**
     * Gets the reader.
     * 
     * @return the reader
     */
    @Override
    public EntityReader getReader()
    {
        return reader;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getQueryImplementor()
     */
    /**
     * Gets the query implementor.
     * 
     * @return the query implementor
     */
    @Override
    public Class<KuduDBQuery> getQueryImplementor()
    {
        return KuduDBQuery.class;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIdGenerator()
     */
    /**
     * Gets the id generator.
     * 
     * @return the id generator
     */
    @Override
    public Generator getIdGenerator()
    {
        return (Generator) KunderaCoreUtils.createNewInstance(KuduDBIdGenerator.class);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientBase#onPersist(com.impetus.kundera.
     * metadata.model.EntityMetadata, java.lang.Object, java.lang.Object,
     * java.util.List)
     */
    /**
     * On persist.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @param id
     *            the id
     * @param rlHolders
     *            the rl holders
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {

        KuduSession session = kuduClient.newSession();
        KuduTable table = null;
        try
        {
            table = kuduClient.openTable(entityMetadata.getTableName());
        }
        catch (Exception e)
        {
            logger.error("Cannot open table : " + entityMetadata.getTableName(), e);
            throw new KunderaException("Cannot open table : " + entityMetadata.getTableName(), e);
        }
        Operation operation = isUpdate ? table.newUpdate() : table.newInsert();
        PartialRow row = operation.getRow();
        populatePartialRow(row, entityMetadata, entity);
        try
        {
            session.apply(operation);
        }
        catch (Exception e)
        {
            logger.error("Cannot insert/update row in table : " + entityMetadata.getTableName(), e);
            throw new KunderaException("Cannot insert/update row in table : " + entityMetadata.getTableName(), e);
        }
        finally
        {
            try
            {
                session.close();
            }
            catch (Exception e)
            {
                logger.error("Cannot close session", e);
                throw new KunderaException("Cannot close session", e);
            }
        }
    }

    /**
     * Populate partial row.
     * 
     * @param row
     *            the row
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     */
    private void populatePartialRow(PartialRow row, EntityMetadata entityMetadata, Object entity)
    {
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(entityMetadata.getPersistenceUnit());
        Class entityClazz = entityMetadata.getEntityClazz();
        EntityType entityType = metaModel.entity(entityClazz);
        Set<Attribute> attributes = entityType.getAttributes();
        Iterator<Attribute> iterator = attributes.iterator();
        iterateAndPopulateRow(row, entity, metaModel, iterator);
    }

    /**
     * Populate partial row for embedded column.
     * 
     * @param row
     *            the row
     * @param embeddable
     *            the embeddable
     * @param EmbEntity
     *            the emb entity
     * @param metaModel
     *            the meta model
     */
    private void populatePartialRowForEmbeddedColumn(PartialRow row, EmbeddableType embeddable, Object EmbEntity,
            MetamodelImpl metaModel)
    {
        Set<Attribute> attributes = embeddable.getAttributes();
        Iterator<Attribute> iterator = attributes.iterator();
        iterateAndPopulateRow(row, EmbEntity, metaModel, iterator);
    }

    /**
     * Iterate and populate row.
     * 
     * @param row
     *            the row
     * @param entity
     *            the entity
     * @param metaModel
     *            the meta model
     * @param iterator
     *            the iterator
     */
    private void iterateAndPopulateRow(PartialRow row, Object entity, MetamodelImpl metaModel,
            Iterator<Attribute> iterator)
    {
        while (iterator.hasNext())
        {
            Attribute attribute = iterator.next();
            Field field = (Field) attribute.getJavaMember();
            Object value = PropertyAccessorHelper.getObject(entity, field);
            if (attribute.getJavaType().isAnnotationPresent(Embeddable.class))
            {
                EmbeddableType emb = metaModel.embeddable(attribute.getJavaType());
                populatePartialRowForEmbeddedColumn(row, emb, value, metaModel);
            }
            else
            {
                Type type = KuduDBValidationClassMapper.getValidTypeForClass(field.getType());
                if (type != null)
                {
                    KuduDBDataHandler.addToRow(row, ((AbstractAttribute) attribute).getJPAColumnName(), value, type);
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientBase#delete(java.lang.Object,
     * java.lang.Object)
     */
    /**
     * Delete.
     * 
     * @param entity
     *            the entity
     * @param pKey
     *            the key
     */
    @Override
    protected void delete(Object entity, Object pKey)
    {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata()
                .getMetamodel(entityMetadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());

        KuduSession session = kuduClient.newSession();
        KuduTable table = null;
        try
        {
            table = kuduClient.openTable(entityMetadata.getTableName());
        }
        catch (Exception e)
        {
            logger.error("Cannot open table : " + entityMetadata.getTableName(), e);
            throw new KunderaException("Cannot open table : " + entityMetadata.getTableName(), e);
        }
        Delete delete = table.newDelete();
        PartialRow row = delete.getRow();
        String idColumnName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getName();

        Field field = (Field) entityType.getAttribute(idColumnName).getJavaMember();
        Object value = PropertyAccessorHelper.getObject(entity, field);
        Type idType = KuduDBValidationClassMapper.getValidTypeForClass(field.getType());

        if (entityType.getAttribute(idColumnName).getJavaType().isAnnotationPresent(Embeddable.class))
        {
            // Composite Id
            EmbeddableType embeddableIdType = metaModel.embeddable(entityType.getAttribute(idColumnName).getJavaType());
            Field[] fields = entityType.getAttribute(idColumnName).getJavaType().getDeclaredFields();
            addPrimaryKeyToRow(row, embeddableIdType, fields, metaModel, value);
        }
        else
        {
            // Simple Id
            KuduDBDataHandler.addToRow(row, ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName(),
                    value, idType);
        }

        try
        {
            session.apply(delete);
        }
        catch (Exception e)
        {
            logger.error("Cannot delete row from table : " + entityMetadata.getTableName(), e);
            throw new KunderaException("Cannot delete row from table : " + entityMetadata.getTableName(), e);
        }
        finally
        {
            try
            {
                session.close();
            }
            catch (Exception e)
            {
                logger.error("Cannot close session", e);
                throw new KunderaException("Cannot close session", e);
            }
        }
    }

    /**
     * Adds the primary key to row.
     *
     * @param row
     *            the row
     * @param embeddable
     *            the embeddable
     * @param fields
     *            the fields
     * @param metaModel
     *            the meta model
     * @param key
     *            the key
     */
    private void addPrimaryKeyToRow(PartialRow row, EmbeddableType embeddable, Field[] fields, MetamodelImpl metaModel,
            Object key)
    {
        for (Field f : fields)
        {
            if (!ReflectUtils.isTransientOrStatic(f))
            {
                Object value = PropertyAccessorHelper.getObject(key, f);
                if (f.getType().isAnnotationPresent(Embeddable.class))
                {
                    // nested
                    addPrimaryKeyToRow(row, (EmbeddableType) metaModel.embeddable(f.getType()),
                            f.getType().getDeclaredFields(), metaModel, value);
                }
                else
                {
                    Attribute attribute = embeddable.getAttribute(f.getName());
                    Type type = KuduDBValidationClassMapper.getValidTypeForClass(f.getType());
                    KuduDBDataHandler.addToRow(row, ((AbstractAttribute) attribute).getJPAColumnName(), value, type);
                }

            }
        }

    }

}
