/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.oraclenosql;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.client.oraclenosql.query.OracleNoSQLQuery;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Implementation of {@link Client} interface for Oracle NoSQL database
 * 
 * @author amresh.singh
 */
public class OracleNoSQLClient extends ClientBase implements Client<OracleNoSQLQuery>
{

    /** The kvstore db. */
    private KVStore kvStore;

    private OracleNoSQLClientFactory factory;

    /** The reader. */
    private EntityReader reader;

    /** The log. */
    private static Log log = LogFactory.getLog(OracleNoSQLClient.class);

    /**
     * Instantiates a new oracle no sqldb client.
     * 
     * @param kvStore
     *            the kv store
     * @param indexManager
     *            the index manager
     * @param reader
     *            the reader
     */
    OracleNoSQLClient(final OracleNoSQLClientFactory factory, EntityReader reader, IndexManager indexManager,
            final KVStore kvStore, Map<String, Object> puProperties, String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
        this.factory = factory;
        this.kvStore = kvStore;
        this.reader = reader;
        this.indexManager = indexManager;
    }

    @Override
    public Object find(Class entityClass, Object key)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClass);
        MetamodelImpl metamodel = (MetamodelImpl) KunderaMetadataManager.getMetamodel(entityMetadata
                .getPersistenceUnit());

        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());

        if (log.isDebugEnabled())
        {
            log.debug("Fetching data from " + entityMetadata.getTableName() + " for PK " + key);
        }

        // Major Key components
        List<String> majorComponents = new ArrayList<String>();
        majorComponents.add(entityMetadata.getTableName());
        majorComponents.add(PropertyAccessorHelper.getString(key));

        Key majorKeyToFind = Key.createKey(majorComponents);

        Object entity = null;

        Map<String, Object> relationMap = null;
        if (entityMetadata.getRelationNames() != null && !entityMetadata.getRelationNames().isEmpty())
        {
            relationMap = new HashMap<String, Object>();
        }

        try
        {
            Iterator<KeyValueVersion> iterator = kvStore.multiGetIterator(Direction.FORWARD, 0, majorKeyToFind, null,
                    null);

            // If a record is found, instantiate entity and set ID value
            if (iterator.hasNext())
            {
                entity = entityMetadata.getEntityClazz().newInstance();
                PropertyAccessorHelper.setId(entity, entityMetadata, key);
            }

            // Populate non-ID attributes
            while (iterator.hasNext())
            {
                KeyValueVersion keyValueVersion = iterator.next();

                String minorKeyFirstPart = keyValueVersion.getKey().getMinorPath().get(0);
                String fieldName = entityMetadata.getFieldName(minorKeyFirstPart);

                if (fieldName != null)
                {
                    Field f = (Field) entityType.getAttribute(fieldName).getJavaMember();

                    if (metamodel.isEmbeddable(f.getType()))
                    {
                        // Populate embedded attribute
                        Class<?> embeddableClass = f.getType();
                        if (metamodel.isEmbeddable(embeddableClass))
                        {
                            String minorKeySecondPart = keyValueVersion.getKey().getMinorPath().get(1);

                            Object embeddedObject = PropertyAccessorHelper.getObject(entity, f);
                            if (embeddedObject == null)
                            {
                                embeddedObject = embeddableClass.newInstance();
                                PropertyAccessorHelper.set(entity, f, embeddedObject);
                            }

                            EmbeddableType embeddableType = metamodel.embeddable(embeddableClass);
                            Attribute columnAttribute = embeddableType.getAttribute(minorKeySecondPart);
                            Field columnField = (Field) columnAttribute.getJavaMember();

                            if (columnField != null)
                            {
                                byte[] value = keyValueVersion.getValue().getValue();
                                PropertyAccessorHelper.set(embeddedObject, columnField, value);
                            }

                        }

                    }
                    else if (entityType.getAttribute(fieldName) != null)
                    {
                        // Populate non-embedded attribute
                        Value v = keyValueVersion.getValue();

                        if (f != null && entityMetadata.getRelation(f.getName()) == null)
                        {
                            PropertyAccessorHelper.set(entity, f, v.getValue());
                        }

                        else if (entityMetadata.getRelationNames() != null
                                && entityMetadata.getRelationNames().contains(minorKeyFirstPart))
                        {
                            Relation relation = entityMetadata.getRelation(f.getName());
                            EntityMetadata associationMetadata = KunderaMetadataManager.getEntityMetadata(relation
                                    .getTargetEntity());
                            relationMap.put(minorKeyFirstPart, PropertyAccessorHelper.getObject(associationMetadata
                                    .getIdAttribute().getBindableJavaType(), v.getValue()));
                        }

                    }
                }

            }

        }
        catch (Exception e)
        {
            log.error(e);
            throw new PersistenceException(e);
        }

        if (relationMap != null && !relationMap.isEmpty())
        {
            EnhanceEntity e = new EnhanceEntity(entity, key, relationMap);
            return e;
        }
        else
        {
            return entity;
        }
    }

    @Override
    public void close()
    {
        // TODO Once pool is implemented this code should not be there.
        // Workaround for pool
        // getIndexManager().flush();

    }

    @Override
    public void delete(Object entity, Object pKey)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());

        List<String> majorKeyComponent = new ArrayList<String>();
        majorKeyComponent.add(entityMetadata.getTableName());
        majorKeyComponent.add(PropertyAccessorHelper.getString(pKey));

        kvStore.multiDelete(Key.createKey(majorKeyComponent), null, null);

        getIndexManager().remove(entityMetadata, entity, pKey.toString());
    }

    /**
     * On persist.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param entity
     *            the entity
     * @param id
     *            the id
     * @param relations
     *            the relations
     * @throws Exception
     *             the exception
     * @throws PropertyAccessException
     *             the property access exception
     */
    @Override
    protected void onPersist(EntityMetadata entityMetadata, Object entity, Object id, List<RelationHolder> rlHolders)
    {
        String schema = entityMetadata.getSchema(); // Irrelevant for this
                                                    // datastore
        String table = entityMetadata.getTableName();

        MetamodelImpl metamodel = (MetamodelImpl) KunderaMetadataManager.getMetamodel(entityMetadata
                .getPersistenceUnit());

        if (log.isDebugEnabled())
        {
            log.debug("Persisting data into " + schema + "." + table + " for " + id);
        }
        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());

        // Major Key component
        List<String> majorKeyComponent = new ArrayList<String>();
        majorKeyComponent.add(table);
        majorKeyComponent.add(PropertyAccessorHelper.getString(id)); // Major
                                                                     // keys
                                                                     // are
                                                                     // always
                                                                     // String

        // Iterate over all Non-ID attributes of this entity (ID is already part
        // of major key)
        Set<Attribute> attributes = entityType.getAttributes();

        for (Attribute attribute : attributes)
        {
            if (!attribute.equals(entityMetadata.getIdAttribute()))
            {
                Class fieldJavaType = ((AbstractAttribute) attribute).getBindableJavaType();

                // If attribute is Embeddable, create minor keys for each
                // attribute it contains
                if (metamodel.isEmbeddable(fieldJavaType))
                {
                    Object embeddedObject = PropertyAccessorHelper.getObject(entity, (Field) attribute.getJavaMember());
                    if (embeddedObject != null)
                    {
                        if (attribute.isCollection())
                        {
                            // ElementCollection is not supported for
                            // OracleNoSQL as of now, ignore for now
                            log.warn("Attribute "
                                    + attribute.getName()
                                    + " will not be persistence because ElementCollection is not supported for OracleNoSQL as of now");
                        }
                        else
                        {
                            String embeddedColumnName = ((AbstractAttribute) attribute).getJPAColumnName();
                            EmbeddableType embeddableType = metamodel.embeddable(fieldJavaType);
                            Set<Attribute> embeddableAttributes = embeddableType.getAttributes();

                            for (Attribute embeddableAttribute : embeddableAttributes)
                            {
                                Field f = (Field) embeddableAttribute.getJavaMember();

                                List<String> minorKeyComponents = new ArrayList<String>();
                                minorKeyComponents.add(embeddedColumnName);
                                minorKeyComponents.add(((AbstractAttribute) embeddableAttribute).getJPAColumnName());

                                // Key
                                Key key = Key.createKey(majorKeyComponent, minorKeyComponents);

                                // Value
                                byte[] valueByteArray = PropertyAccessorHelper.get(embeddedObject, f);
                                Value value = Value.createValue(valueByteArray);

                                kvStore.put(key, value);
                            }
                        }
                    }
                }

                // All other non-embeddable agttributes (ignore associations, as
                // they will be store by separate call)
                else if (!attribute.isAssociation())
                {
                    Field field = (Field) attribute.getJavaMember();
                    String columnName = ((AbstractAttribute) attribute).getJPAColumnName();

                    // Key
                    Key key = Key.createKey(majorKeyComponent, columnName);

                    // Value
                    byte[] valueByteArray = PropertyAccessorHelper.get(entity, field);
                    Value value = Value.createValue(valueByteArray);

                    kvStore.put(key, value);
                }
            }
        }

        // Iterate over relations
        if (rlHolders != null && !rlHolders.isEmpty())
        {
            for (RelationHolder rh : rlHolders)
            {
                String relationName = rh.getRelationName();
                Object valueObj = rh.getRelationValue();

                if (!StringUtils.isEmpty(relationName) && valueObj != null)
                {
                    // Key
                    Key key = Key.createKey(majorKeyComponent, relationName);

                    // Value
                    byte[] valueInBytes = PropertyAccessorHelper.getBytes(valueObj);
                    Value value = Value.createValue(valueInBytes);

                    kvStore.put(key, value);
                }
            }
        }
    }

    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
        String joinTableName = joinTableData.getJoinTableName();
        String joinColumnName = joinTableData.getJoinColumnName();
        String invJoinColumnName = joinTableData.getInverseJoinColumnName();
        Map<Object, Set<Object>> joinTableRecords = joinTableData.getJoinTableRecords();

        /**
         * There will be two kinds of major keys 1.
         * /Join_Table_Name/Join_Column_Name/Primary_Key_On_Owning_Side 2.
         * /Join_Table_Name/Inverse_Join_Column_Name/Primary_Key_On_Other_Side
         * 
         * Minor keys for both will be list of Primary keys at the opposite
         * side, value will always be null
         */

        for (Object pk : joinTableRecords.keySet())
        {
            // Save Join Column ---> inverse Join Column mapping
            List<String> majorKeysForJoinColumn = new ArrayList<String>();

            majorKeysForJoinColumn.add(joinTableName);
            // majorKeysForJoinColumn.add(joinColumnName);
            majorKeysForJoinColumn.add(PropertyAccessorHelper.getString(pk));

            Set<Object> values = joinTableRecords.get(pk);
            List<String> minorKeysForJoinColumn = new ArrayList<String>();

            for (Object childId : values)
            {
                minorKeysForJoinColumn.add(PropertyAccessorHelper.getString(childId));

                // Save Invese join Column ---> Join Column mapping
                List<String> majorKeysForInvJoinColumn = new ArrayList<String>();
                majorKeysForInvJoinColumn.add(joinTableName);
                // majorKeysForInvJoinColumn.add(invJoinColumnName);
                majorKeysForInvJoinColumn.add(PropertyAccessorHelper.getString(childId));

                Key key = Key.createKey(majorKeysForInvJoinColumn, PropertyAccessorHelper.getString(pk));
                kvStore.put(key, Value.createValue(invJoinColumnName.getBytes())); // Value
                                                                                   // will
                                                                                   // be
                                                                                   // null
            }

            Key key = Key.createKey(majorKeysForJoinColumn, minorKeysForJoinColumn);
            kvStore.put(key, Value.createValue(joinColumnName.getBytes())); // Value
                                                                            // will
                                                                            // be
                                                                            // null
        }
    }

    @Override
    public <E> List<E> findAll(Class<E> entityClass, Object... keys)
    {
        List<E> results = new ArrayList<E>();

        for (Object key : keys)
        {
            results.add((E) find(entityClass, key));
        }

        return results;
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        return null;
    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue)
    {
        List<E> foreignKeys = new ArrayList<E>();

        // Major Key components
        List<String> majorComponents = new ArrayList<String>();
        majorComponents.add(tableName);
        // majorComponents.add(pKeyColumnName);
        majorComponents.add(PropertyAccessorHelper.getString(pKeyColumnValue));
        Key majorKeyToFind = Key.createKey(majorComponents);

        Iterator<KeyValueVersion> iterator = kvStore.multiGetIterator(Direction.FORWARD, 0, majorKeyToFind, null, null);

        try
        {
            while (iterator.hasNext())
            {
                KeyValueVersion keyValueVersion = iterator.next();

                String value = new String(keyValueVersion.getValue().getValue(), "UTF-8");
                if (value != null && value.equals(pKeyColumnName))
                {
                    Iterator<String> minorKeyIterator = keyValueVersion.getKey().getMinorPath().iterator();

                    while (minorKeyIterator.hasNext())
                    {
                        String minorKey = minorKeyIterator.next();
                        foreignKeys.add((E) minorKey);
                    }
                }

            }
        }
        catch (UnsupportedEncodingException e)
        {
            e.printStackTrace();
        }
        return foreignKeys;
    }

    @Override
    public Object[] findIdsByColumn(String schemaName, String tableName, String pKeyName, String columnName,
            Object columnValue, Class entityClazz)
    {
        String valueAsStr = PropertyAccessorHelper.getString(columnValue);
        Set<String> results = new HashSet<String>();

        // Major Key components
        List<String> majorComponents = new ArrayList<String>();
        majorComponents.add(tableName);
        // majorComponents.add(columnName);
        majorComponents.add(PropertyAccessorHelper.getString(valueAsStr));
        Key majorKeyToFind = Key.createKey(majorComponents);

        Iterator<KeyValueVersion> iterator = kvStore.multiGetIterator(Direction.FORWARD, 0, majorKeyToFind, null, null);
        try
        {
            while (iterator.hasNext())
            {
                KeyValueVersion keyValueVersion = iterator.next();
                String value = new String(keyValueVersion.getValue().getValue(), "UTF-8");

                if (value != null && value.equals(columnName))
                {
                    Iterator<String> minorKeyIterator = keyValueVersion.getKey().getMinorPath().iterator();

                    while (minorKeyIterator.hasNext())
                    {
                        String minorKey = minorKeyIterator.next();
                        results.add(minorKey);
                    }
                }

            }
        }
        catch (UnsupportedEncodingException e)
        {
            e.printStackTrace();
        }

        if (results != null && !results.isEmpty())
        {
            return results.toArray(new Object[0]);
        }
        return null;
    }

    @Override
    public void deleteByColumn(String schemaName, String tableName, String columnName, Object columnValue)
    {
        List<String> majorKeyComponent = new ArrayList<String>();
        majorKeyComponent.add(tableName);
        majorKeyComponent.add(PropertyAccessorHelper.getString(columnValue));
        Key majorKey = Key.createKey(majorKeyComponent);
        boolean deleteApplicableOnMajorKey = false;

        // Store minor keys in an array before deleting
        List<String> minorKeys = new ArrayList<String>();
        Iterator<KeyValueVersion> iterator = kvStore.multiGetIterator(Direction.FORWARD, 0, majorKey, null, null);
        try
        {
            while (iterator.hasNext())
            {
                KeyValueVersion keyValueVersion = iterator.next();

                String value = new String(keyValueVersion.getValue().getValue(), "UTF-8");
                if (value != null && value.equals(columnName))
                {
                    deleteApplicableOnMajorKey = true;

                    Iterator<String> minorKeyIterator = keyValueVersion.getKey().getMinorPath().iterator();

                    while (minorKeyIterator.hasNext())
                    {
                        String minorKey = minorKeyIterator.next();
                        minorKeys.add(minorKey);
                    }
                }

            }
        }
        catch (UnsupportedEncodingException e)
        {
            e.printStackTrace();
        }

        if (deleteApplicableOnMajorKey)
        {
            // Delete This columnValue as major key
            kvStore.multiDelete(majorKey, null, null);

            // Delete all minor keys that contain this columnValue
            for (String key : minorKeys)
            {
                List<String> majorKeys = new ArrayList<String>();
                majorKeys.add(tableName);
                majorKeys.add(PropertyAccessorHelper.getString(key));
                Key majorAndMinorKeys = Key.createKey(majorKeys, PropertyAccessorHelper.getString(columnValue));
                kvStore.multiDelete(majorAndMinorKeys, null, null);
            }
        }

    }

    @Override
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        return null;
    }

    @Override
    public EntityReader getReader()
    {
        return reader;
    }

    @Override
    public Class<OracleNoSQLQuery> getQueryImplementor()
    {
        return OracleNoSQLQuery.class;
    }

}
