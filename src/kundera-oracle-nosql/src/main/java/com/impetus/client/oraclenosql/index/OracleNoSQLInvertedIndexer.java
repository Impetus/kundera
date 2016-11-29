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
package com.impetus.client.oraclenosql.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;

import com.impetus.client.oraclenosql.OracleNOSQLConstants;
import com.impetus.client.oraclenosql.OracleNoSQLDataHandler;
import com.impetus.client.oraclenosql.query.OracleNoSQLQueryInterpreter;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.index.Indexer;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;

/**
 * Inverted Index implementation of {@link Indexer}
 * 
 * @author amresh.singh
 */
public class OracleNoSQLInvertedIndexer implements Indexer
{

    private KVStore kvStore;

    private OracleNoSQLDataHandler handler;

    @Override
    public void index(Class entityClazz, EntityMetadata m, Map<String, Object> values, Object entityId,
            final Class parentClazz)
    {
        String idColumnName = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
        Object id = values.get(idColumnName);

        for (String column : values.keySet())
        {
            Object valueObject = values.get(column);

            List<String> majorKeyComponents = new ArrayList<String>();
            majorKeyComponents.add(getIndexTableName(m));
            majorKeyComponents.add(column);
            majorKeyComponents.add(PropertyAccessorHelper.getString(valueObject));

            String minorKey = PropertyAccessorHelper.getString(id);

            Key key = Key.createKey(majorKeyComponents, minorKey);
            byte[] valueByteArray = PropertyAccessorHelper.getBytes(id);

            kvStore.put(key, Value.createValue(valueByteArray));
        }
    }

    @Override
    public Map<String, Object> search(Class<?> clazz, EntityMetadata m, String luceneQuery, int start, int count)
    {
        throw new UnsupportedOperationException("This method is not supported for OracleNoSQL.");
    }

    @Override
    public Map<String, Object> search(String query, Class<?> parentClass, EntityMetadata parentMetadata,
            Class<?> childClass, EntityMetadata childMetadata, Object entityId, int start, int count)
    {
        String secIndexName = getIndexTableName(childMetadata);
        String parentIdColumnName = ((AbstractAttribute) parentMetadata.getIdAttribute()).getJPAColumnName();
        String childIdColumnName = ((AbstractAttribute) childMetadata.getIdAttribute()).getJPAColumnName();
        String id = PropertyAccessorHelper.getString(entityId);

        List<String> majorComponents = new ArrayList<String>();
        majorComponents.add(secIndexName);
        majorComponents.add(parentIdColumnName);
        majorComponents.add(id);

        Key majorKeyToFind = Key.createKey(majorComponents);

        Iterator<KeyValueVersion> iterator = kvStore.multiGetIterator(Direction.FORWARD, 0, majorKeyToFind, null, null);

        Map<String, Object> results = new HashMap<String, Object>();

        while (iterator.hasNext())
        {
            KeyValueVersion keyValueVersion = iterator.next();
            String minorKey = keyValueVersion.getKey().getMinorPath().get(0);

            PropertyAccessor accessor = PropertyAccessorFactory
                    .getPropertyAccessor(childMetadata.getIdAttribute().getBindableJavaType());

            byte[] idByteArr = keyValueVersion.getValue().getValue();
            Object keyObj = accessor.fromBytes(childMetadata.getIdAttribute().getBindableJavaType(), idByteArr);

            results.put(childIdColumnName + "|" + minorKey, keyObj);
        }

        return results;
    }

    public <E> Set<E> executeQuery(OracleNoSQLQueryInterpreter interpreter, Class<?> entityClass,
            EntityMetadata entityMetadata)
    {
        String idColumnName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
        String secIndexName = getIndexTableName(entityMetadata);

        Set<Object> results = new HashSet<Object>();
        Set<Object> foundKeys = new HashSet<Object>();
        String interClauseOperator = null;

        Queue filterClauseQueue = interpreter.getClauseQueue();

        for (Object clause : filterClauseQueue)
        {
            if (clause instanceof FilterClause)
            {
                foundKeys = new HashSet<Object>();

                String columnName = ((FilterClause) clause).getProperty();
                String condition = ((FilterClause) clause).getCondition();
                Object value = ((FilterClause) clause).getValue();

                if (columnName.equals(((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName())
                        && condition.equals("="))
                {
                    Object idValue = PropertyAccessorHelper.fromSourceToTargetClass(
                            entityMetadata.getIdAttribute().getJavaType(), String.class, value);
                    foundKeys.add(idValue);
                }
                else
                {
                    List<String> majorComponents = new ArrayList<String>();
                    majorComponents.add(secIndexName);
                    majorComponents.add(columnName);

                    KeyRange range = null;
                    Iterator<KeyValueVersion> iterator = null;

                    if (condition.equals("="))
                    {
                        majorComponents.add(PropertyAccessorHelper.getString(value));
                        Key majorKeyToFind = Key.createKey(majorComponents);
                        iterator = kvStore.multiGetIterator(Direction.FORWARD, 0, majorKeyToFind, range, null);
                    }
                    else if (condition.equals(">"))
                    {
                        range = new KeyRange(value.toString(), false, null, true);
                        Key majorKeyToFind = Key.createKey(majorComponents);
                        iterator = kvStore.storeIterator(Direction.UNORDERED, 0, majorKeyToFind, range, null);
                    }
                    else if (condition.equals("<"))
                    {
                        range = new KeyRange(null, true, value.toString(), false);
                        Key majorKeyToFind = Key.createKey(majorComponents);
                        iterator = kvStore.storeIterator(Direction.UNORDERED, 0, majorKeyToFind, range, null);
                    }
                    else if (condition.equals(">="))
                    {
                        range = new KeyRange(value.toString(), true, null, true);
                        Key majorKeyToFind = Key.createKey(majorComponents);
                        iterator = kvStore.storeIterator(Direction.UNORDERED, 0, majorKeyToFind, range, null);
                    }
                    else if (condition.equals("<="))
                    {
                        range = new KeyRange(null, true, value.toString(), true);
                        Key majorKeyToFind = Key.createKey(majorComponents);
                        iterator = kvStore.storeIterator(Direction.UNORDERED, 0, majorKeyToFind, range, null);
                    }

                    while (iterator.hasNext())
                    {
                        KeyValueVersion keyValueVersion = iterator.next();
                        String minorKey = keyValueVersion.getKey().getMinorPath().get(0);

                        PropertyAccessor accessor = PropertyAccessorFactory
                                .getPropertyAccessor(entityMetadata.getIdAttribute().getBindableJavaType());

                        byte[] idByteArr = keyValueVersion.getValue().getValue();
                        Object keyObj = accessor.fromBytes(entityMetadata.getIdAttribute().getBindableJavaType(),
                                idByteArr);

                        foundKeys.add(keyObj);
                    }

                }
            }
            else if (clause instanceof String)
            {
                interClauseOperator = clause.toString().trim();
            }

            addToResults(results, foundKeys, interClauseOperator);
        }

        return (Set<E>) results;

    }

    @Override
    public void unIndex(Class entityClazz, Object entity, EntityMetadata entityMetadata, MetamodelImpl metamodel)
    {
        String indexTableName = getIndexTableName(entityMetadata);

        // byte[] id = PropertyAccessorHelper.get(entity,
        // (Field)entityMetadata.getIdAttribute().getJavaMember());
        Object id = PropertyAccessorHelper.getId(entity, entityMetadata);
        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());
        Set<Attribute> attributes = entityType.getSingularAttributes();

        for (Attribute attribute : attributes)
        {
            Class fieldJavaType = ((AbstractAttribute) attribute).getBindableJavaType();

            if (!attribute.isAssociation() && !metamodel.isEmbeddable(fieldJavaType))
            {
                String columnName = ((AbstractAttribute) attribute).getJPAColumnName();

                List<String> majorComponents = new ArrayList<String>();
                majorComponents.add(indexTableName);
                majorComponents.add(columnName);

                Key key = Key.createKey(majorComponents);
                Iterator<KeyValueVersion> iterator = kvStore.storeIterator(Direction.UNORDERED, 0, key, null, null);

                while (iterator.hasNext())
                {
                    KeyValueVersion keyValueVersion = iterator.next();
                    Key keytoDelete = keyValueVersion.getKey();
                    byte[] value = keyValueVersion.getValue().getValue();
                    Object valueObject = PropertyAccessorHelper.getObject(
                            ((AbstractAttribute) entityMetadata.getIdAttribute()).getBindableJavaType(), value);

                    if (valueObject.equals(id))
                    {
                        // Delete this key
                        kvStore.multiDelete(keytoDelete, null, null);
                    }
                }
            }
        }

    }

    @Override
    public void close()
    {
    }

    private void addToResults(Set results, Set resultsToAdd, String operation)
    {
        if (resultsToAdd == null || resultsToAdd.isEmpty())
        {
            return;
        }

        if (operation == null)
        {
            results.addAll(resultsToAdd);
        }
        else if (operation.equalsIgnoreCase("OR"))
        {
            results.addAll(resultsToAdd);
        }
        else if (operation.equalsIgnoreCase("AND"))
        {
            if (results.isEmpty())
            {
                results.addAll(resultsToAdd);
            }
            else
            {
                results.retainAll(resultsToAdd);
            }
        }

        resultsToAdd.clear();
    }

    /**
     * @return the kvStore
     */
    public KVStore getKvStore()
    {
        return kvStore;
    }

    /**
     * @param kvStore
     *            the kvStore to set
     */
    public void setKvStore(KVStore kvStore)
    {
        this.kvStore = kvStore;
    }

    /**
     * @param handler
     *            the handler to set
     */
    public void setHandler(OracleNoSQLDataHandler handler)
    {
        this.handler = handler;
    }

    /**
     * @param entityMetadata
     * @return
     */
    protected String getIndexTableName(EntityMetadata entityMetadata)
    {
        return entityMetadata.getIndexName() + OracleNOSQLConstants.SECONDARY_INDEX_SUFFIX;
    }

    @Override
    public Map<String, Object> search(KunderaMetadata kunderaMetadata, KunderaQuery kunderaQuery,
            PersistenceDelegator persistenceDelegator, EntityMetadata m, int firstResult, int maxResults)
    {
        throw new KunderaException("Unsupported Method");
    }
}