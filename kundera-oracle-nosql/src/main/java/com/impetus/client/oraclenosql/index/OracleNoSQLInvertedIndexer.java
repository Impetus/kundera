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
import java.util.Set;

import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;

import com.impetus.client.oraclenosql.query.OracleNoSQLQueryInterpreter;
import com.impetus.kundera.index.Indexer;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessor;
import com.impetus.kundera.property.PropertyAccessorFactory;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Inverted Index implementation of {@link Indexer}
 * 
 * @author amresh.singh
 */
public class OracleNoSQLInvertedIndexer implements Indexer
{

    KVStore kvStore;    

    @Override
    public void index(Class entityClazz, Map<String, Object> values)
    {
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(entityClazz);
        String idColumnName = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
        Object id = values.get(idColumnName);

        for (String column : values.keySet())
        {
            Object value = values.get(column);

            List<String> majorKeyComponents = new ArrayList<String>();
            majorKeyComponents.add(getIndexTableName(m));
            majorKeyComponents.add(column);
            majorKeyComponents.add(PropertyAccessorHelper.getString(value));

            String minorKey = PropertyAccessorHelper.getString(id);

            Key key = Key.createKey(majorKeyComponents, minorKey);
            
            byte[] valueByteArray = PropertyAccessorHelper.getBytes(id);           
            kvStore.put(key, Value.createValue(valueByteArray));
        }
    }

    @Override
    public Map<String, Object> search(String queryString, int start, int count)
    {

        return null;
    }

    @Override
    public Map<String, Object> search(Class<?> parentClass, Class<?> childClass, Object entityId, int start, int count)
    {
        EntityMetadata parentMetadata = KunderaMetadataManager.getEntityMetadata(parentClass);
        EntityMetadata childMetadata = KunderaMetadataManager.getEntityMetadata(childClass);
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

            PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(childMetadata.getIdAttribute()
                    .getBindableJavaType());

            byte[] idByteArr = keyValueVersion.getValue().getValue();
            Object keyObj = accessor.fromBytes(childMetadata.getIdAttribute().getBindableJavaType(), idByteArr);
            
            results.put(childIdColumnName + "|" + minorKey, keyObj);
        }

        return results;
    }

    
    
    public <E> Set<E> executeQuery(OracleNoSQLQueryInterpreter interpreter, Class<?> entityClass)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClass);
        String idColumnName = ((AbstractAttribute) entityMetadata.getIdAttribute()).getJPAColumnName();
        String secIndexName = getIndexTableName(entityMetadata);
        Map<String, Object> filterConditions = interpreter.getFilterConditions();
        Set<Object> results = new HashSet<Object>();

        for (String columnName : filterConditions.keySet())
        {
            Set<Object> foundKeys = new HashSet<Object>();
            
            Object value = filterConditions.get(columnName);
            if (interpreter.isById() && columnName.equals(idColumnName))
            {
                foundKeys.add(value);
            }
            else
            {
                List<String> majorComponents = new ArrayList<String>();
                majorComponents.add(secIndexName);
                majorComponents.add(columnName);
                majorComponents.add(PropertyAccessorHelper.getString(value));

                Key majorKeyToFind = Key.createKey(majorComponents);

                Iterator<KeyValueVersion> iterator = kvStore.multiGetIterator(Direction.FORWARD, 0, majorKeyToFind,
                        null, null);
                
                while (iterator.hasNext())
                {
                    KeyValueVersion keyValueVersion = iterator.next();
                    String minorKey = keyValueVersion.getKey().getMinorPath().get(0);

                    PropertyAccessor accessor = PropertyAccessorFactory.getPropertyAccessor(entityMetadata
                            .getIdAttribute().getBindableJavaType());

                    byte[] idByteArr = keyValueVersion.getValue().getValue();
                    Object keyObj = accessor
                            .fromBytes(entityMetadata.getIdAttribute().getBindableJavaType(), idByteArr);

                    foundKeys.add(keyObj);
                }
            }
            
            addToPrimaryKeySet(results, foundKeys, interpreter.getOperator());            
        }

        return (Set<E>) results;

    }

    @Override
    public void unIndex(Class entityClazz, Object key)
    {

    }

    @Override
    public void close()
    {
    }
    
    private void addToPrimaryKeySet(Set results, Set keysToAdd, String operation)
    {
        if(operation == null)
        {
            results.addAll(keysToAdd);
        }
        else if(operation.equalsIgnoreCase("OR"))
        {
            results.addAll(keysToAdd);
        }
        else if(operation.equalsIgnoreCase("AND"))
        {
            if(results.isEmpty())
            {
                results.addAll(keysToAdd);
            }
            else
            {
                results.retainAll(keysToAdd);
            }            
        }
        
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
     * @param entityMetadata
     * @return
     */
    protected String getIndexTableName(EntityMetadata entityMetadata)
    {
        return entityMetadata.getIndexName() + "_idx";
    }

}
