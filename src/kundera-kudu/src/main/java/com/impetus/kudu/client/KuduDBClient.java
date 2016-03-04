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
package com.impetus.kudu.client;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.kududb.ColumnSchema;
import org.kududb.Type;
import org.kududb.client.ColumnRangePredicate;
import org.kududb.client.Delete;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduScanner;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import org.kududb.client.Operation;
import org.kududb.client.PartialRow;
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kudu.query.KuduDBQuery;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.ClientPropertiesSetter;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * The Class KuduDBClient.
 * 
 * @author karthikp.manchala
 */
public class KuduDBClient extends ClientBase implements Client<KuduDBQuery>, ClientPropertiesSetter {

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
     * @param reader
     *            the reader
     * @param properties
     *            the properties
     * @param persistenceUnit
     *            the persistence unit
     * @param kuduClient
     *            the kudu client
     */
    protected KuduDBClient(KunderaMetadata kunderaMetadata, EntityReader reader, Map<String, Object> properties,
        String persistenceUnit, KuduClient kuduClient) {
        super(kunderaMetadata, properties, persistenceUnit);
        this.reader = reader;
        this.kuduClient = kuduClient;
    }

    /**
     * Gets the kudu client.
     * 
     * @return the kudu client
     */
    public KuduClient getKuduClient() {
        return kuduClient;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.ClientPropertiesSetter#populateClientProperties(com.impetus.kundera.client.Client,
     * java.util.Map)
     */
    @Override
    public void populateClientProperties(Client client, Map<String, Object> properties) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class, java.lang.Object)
     */
    @Override
    public Object find(Class entityClass, Object key) {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entityClass);

        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(entityMetadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());

        String idColumnName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();

        Field field = (Field) entityType.getAttribute(idColumnName).getJavaMember();

        Type idType = KuduDBValidationClassMapper.getValidTypeForClass(field.getType());

        KuduTable table;
        try {
            table = kuduClient.openTable(entityMetadata.getTableName());
        } catch (Exception e) {
            logger.error("Cannot open table : " + entityMetadata.getTableName(), e);
            throw new KunderaException("Cannot open table : " + entityMetadata.getTableName(), e);
        }
        ColumnSchema column = new ColumnSchema.ColumnSchemaBuilder(idColumnName, idType).build();
        ColumnRangePredicate predicate = new ColumnRangePredicate(column);
        KuduDBDataHandler.setPredicateLowerBound(predicate, idType, key);
        KuduDBDataHandler.setPredicateUpperBound(predicate, idType, key);
        KuduScanner scanner = kuduClient.newScannerBuilder(table).addColumnRangePredicate(predicate).build();
        Object entity = null;
        while (scanner.hasMoreRows()) {
            RowResultIterator results;
            try {
                results = scanner.nextRows();
            } catch (Exception e) {
                logger.error("Cannot get results from table : " + entityMetadata.getTableName(), e);
                throw new KunderaException("Cannot get results from table : " + entityMetadata.getTableName(), e);
            }

            while (results.hasNext()) {
                RowResult result = results.next();
                entity = KunderaCoreUtils.createNewInstance(entityClass);
                // populate RowResult to entity object and return
                populateEntity(entity, result, entityType);
                logger.debug(result.rowToString());
            }
        }
        return entity;
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
     */
    public void populateEntity(Object entity, RowResult result, EntityType entityType) {
        for (ColumnSchema column : result.getSchema().getColumns()) {
            Attribute attribute = entityType.getAttribute(column.getName());
            Field field = (Field) attribute.getJavaMember();
            PropertyAccessorHelper.set(entity, field,
                KuduDBDataHandler.getColumnValue(result, ((AbstractAttribute) attribute).getJPAColumnName()));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findAll(java.lang.Class, java.lang.String[], java.lang.Object[])
     */
    @Override
    public <E> List<E> findAll(Class<E> entityClass, String[] columnsToSelect, Object... keys) {
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#find(java.lang.Class, java.util.Map)
     */
    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.client.Client#persistJoinTable(com.impetus.kundera.persistence.context.jointable.JoinTableData
     * )
     */
    @Override
    public void persistJoinTable(JoinTableData joinTableData) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getColumnsById(java.lang.String, java.lang.String, java.lang.String,
     * java.lang.String, java.lang.Object, java.lang.Class)
     */
    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
        Object pKeyColumnValue, Class columnJavaType) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findIdsByColumn(java.lang.String, java.lang.String, java.lang.String,
     * java.lang.String, java.lang.Object, java.lang.Class)
     */
    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
        Object columnValue, Class entityClazz) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#deleteByColumn(java.lang.String, java.lang.String, java.lang.String,
     * java.lang.Object)
     */
    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#findByRelation(java.lang.String, java.lang.Object, java.lang.Class)
     */
    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getReader()
     */
    @Override
    public EntityReader getReader() {
        // TODO Auto-generated method stub
        return reader;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getQueryImplementor()
     */
    @Override
    public Class<KuduDBQuery> getQueryImplementor() {
        return KuduDBQuery.class;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#getIdGenerator()
     */
    @Override
    public Generator getIdGenerator() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientBase#onPersist(com.impetus.kundera.metadata.model.EntityMetadata,
     * java.lang.Object, java.lang.Object, java.util.List)
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders) {

        KuduSession session = kuduClient.newSession();
        System.out.println(session.getTimeoutMillis());
        session.setTimeoutMillis(1000 * 60 * 5);
        KuduTable table = null;
        try {
            table = kuduClient.openTable(entityMetadata.getTableName());
        } catch (Exception e) {
            logger.error("Cannot open table : " + entityMetadata.getTableName(), e);
            throw new KunderaException("Cannot open table : " + entityMetadata.getTableName(), e);
        }
        Operation operation = null;
        if (isUpdate) {
            // update
            operation = table.newUpdate();
        } else {
            // populate insert operation
            operation = table.newInsert();
        }

        PartialRow row = operation.getRow();
        populatePartialRow(row, entityMetadata, entity);
        try {
            session.apply(operation);
        } catch (Exception e) {
            logger.error("Cannot insert row in table : " + entityMetadata.getTableName(), e);
            throw new KunderaException("Cannot insert row in table : " + entityMetadata.getTableName(), e);
        } finally {
            try {
                session.close();
            } catch (Exception e) {
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
    private void populatePartialRow(PartialRow row, EntityMetadata entityMetadata, Object entity) {
        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(entityMetadata.getPersistenceUnit());
        Class entityClazz = entityMetadata.getEntityClazz();
        EntityType entityType = metaModel.entity(entityClazz);
        Set<Attribute> attributes = entityType.getAttributes();
        Iterator<Attribute> iterator = attributes.iterator();
        while (iterator.hasNext()) {
            Attribute attribute = iterator.next();
            Field field = (Field) attribute.getJavaMember();
            // if(attribute.equals(entityMetadata.getIdAttribute()) ||
            // ((AbstractAttribute) attribute).getJPAColumnName().equals(
            // ((AbstractAttribute)
            // entityMetadata.getIdAttribute()).getJPAColumnName())){
            Object value = PropertyAccessorHelper.getObject(entity, field);
            Type type = KuduDBValidationClassMapper.getValidTypeForClass(field.getType());
            KuduDBDataHandler.addToRow(row, ((AbstractAttribute) attribute).getJPAColumnName(), value, type);
            // }
            // else{
            //
            // }
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.ClientBase#delete(java.lang.Object, java.lang.Object)
     */
    @Override
    protected void delete(Object entity, Object pKey) {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());
        MetamodelImpl metaModel =
            (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(entityMetadata.getPersistenceUnit());
        EntityType entityType = metaModel.entity(entityMetadata.getEntityClazz());

        KuduSession session = kuduClient.newSession();
        System.out.println(session.getTimeoutMillis());
        session.setTimeoutMillis(1000 * 60 * 5);
        KuduTable table = null;
        try {
            table = kuduClient.openTable(entityMetadata.getTableName());
        } catch (Exception e) {
            logger.error("Cannot open table : " + entityMetadata.getTableName(), e);
            throw new KunderaException("Cannot open table : " + entityMetadata.getTableName(), e);
        }
        // populate delete operation
        Delete delete = table.newDelete();
        PartialRow row = delete.getRow();
        Field field =
            (Field) entityType.getAttribute(((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName())
                .getJavaMember();
        Object value = PropertyAccessorHelper.getObject(entity, field);
        Type type = KuduDBValidationClassMapper.getValidTypeForClass(field.getType());
        KuduDBDataHandler.addToRow(row, ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName(),
            value, type);

        try {
            session.apply(delete);
        } catch (Exception e) {
            logger.error("Cannot delete row from table : " + entityMetadata.getTableName(), e);
            throw new KunderaException("Cannot delete row from table : " + entityMetadata.getTableName(), e);
        } finally {
            try {
                session.close();
            } catch (Exception e) {
                logger.error("Cannot close session", e);
                throw new KunderaException("Cannot close session", e);
            }
        }
    }

}
