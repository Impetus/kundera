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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.Value;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.tuple.entity.EntityMetamodel;

import com.impetus.client.oraclenosql.query.OracleNoSQLQuery;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.db.RelationHolder;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.context.jointable.JoinTableData;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * <Prove description of functionality provided by this Type> 
 * @author amresh.singh
 */
public class OracleNoSQLClient extends ClientBase implements Client<OracleNoSQLQuery> {

    /** The is connected. */
    // private boolean isConnected;

    /** The kvstore db. */
    private KVStore kvStore;
    
    OracleNoSQLClientFactory factory;

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
    OracleNoSQLClient(final OracleNoSQLClientFactory factory, final KVStore kvStore, Map<String, Object> puProperties, String persistenceUnit)
    {
        this.persistenceUnit = persistenceUnit;
        this.factory = factory;
        this.kvStore = kvStore;    

    }
    
    @Override
    public Object find(Class entityClass, Object key)
    {
        
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entityClass);
        Metamodel metamodel = KunderaMetadataManager.getMetamodel(entityMetadata.getPersistenceUnit());
        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());   
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
        Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();
        MetadataUtils.populateColumnAndSuperColumnMaps(entityMetadata, columnNameToFieldMap, superColumnNameToFieldMap);
        
        log.debug("Fetching data from " + entityMetadata.getTableName() + " for PK " + key);

        List<String> majorComponents = new ArrayList<String>();
        majorComponents.add(entityMetadata.getTableName());
        majorComponents.add(key.toString());
        Key findKey = Key.createKey(majorComponents);
        Object entity = null;
        try {
            entity = entityMetadata.getEntityClazz().newInstance();
            PropertyAccessorHelper.setId(entity, entityMetadata, key.toString());
            Iterator<KeyValueVersion> i = kvStore.multiGetIterator(Direction.FORWARD, 0, findKey, null, null);

            
            
            
            while (i.hasNext()) {
                KeyValueVersion keyValueVersion = i.next();
                String columnName = keyValueVersion.getKey().getMinorPath().get(0);

                Value v = keyValueVersion.getValue();
                PropertyAccessorHelper.set(entity, columnNameToFieldMap.get(columnName), v.getValue());
                // Do some work with the Value here

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return entity;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.client.Client#close()
     */
    @Override
    public void close() {
        // TODO Once pool is implemented this code should not be there.
        // Workaround for pool
        getIndexManager().flush();

    }

    @Override
    public void delete(Object entity, Object pKey)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());

        Key key = Key.createKey(pKey.toString());
        kvStore.delete(key);
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
        String dbName = entityMetadata.getSchema();
        String entityName = entityMetadata.getTableName();
        
        Metamodel metamodel = KunderaMetadataManager.getMetamodel(entityMetadata.getPersistenceUnit());

        log.debug("Persisting data into " + dbName + "." + entityName + " for " + id);
        Key key = null;
        byte[] valueString = null;
        Value value = null;
        
        
        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());
        // Iterate over, entity attributes
        Set<Attribute> attributes = entityType.getSingularAttributes();
        
        for(Attribute attribute : attributes)
        {
            Field field = (Field) attribute.getJavaMember();
            String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
            
            List<String> majorKeyComponent = new ArrayList<String>();
            majorKeyComponent.add(entityName);
            majorKeyComponent.add(id.toString());
            valueString = PropertyAccessorHelper.get(entity, field);
            key = Key.createKey(majorKeyComponent, columnName);
            value = Value.createValue(valueString);
            kvStore.put(key, value);
        }       

    }  
    
    

    @Override
    public <E> List<E> findAll(Class<E> entityClass, Object... keys)
    {
        return null;
    }

    @Override
    public <E> List<E> find(Class<E> entityClass, Map<String, String> embeddedColumnMap)
    {
        return null;
    }


    @Override
    public void persistJoinTable(JoinTableData joinTableData)
    {
    }

    @Override
    public <E> List<E> getColumnsById(String schemaName, String tableName, String pKeyColumnName, String columnName,
            Object pKeyColumnValue)
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
    public List<Object> findByRelation(String colName, Object colValue, Class entityClazz)
    {
        return null;
    }

    @Override
    public EntityReader getReader()
    {
        return null;
    }

    @Override
    public Class<OracleNoSQLQuery> getQueryImplementor()
    {
        return OracleNoSQLQuery.class;
    } 

}



