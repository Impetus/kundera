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
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.Relation;
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
    OracleNoSQLClient(final OracleNoSQLClientFactory factory, EntityReader reader, IndexManager indexManager, final KVStore kvStore, Map<String, Object> puProperties, String persistenceUnit)
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
        MetamodelImpl metamodel = (MetamodelImpl) KunderaMetadataManager.getMetamodel(entityMetadata.getPersistenceUnit());
        
        Map<String, Field> columnNameToFieldMap = new HashMap<String, Field>();
        Map<String, Field> superColumnNameToFieldMap = new HashMap<String, Field>();
        MetadataUtils.populateColumnAndSuperColumnMaps(entityMetadata, columnNameToFieldMap, superColumnNameToFieldMap);
        
        log.debug("Fetching data from " + entityMetadata.getTableName() + " for PK " + key);

        //Major Key components
        List<String> majorComponents = new ArrayList<String>();
        majorComponents.add(entityMetadata.getTableName());
        majorComponents.add(PropertyAccessorHelper.getString(key));
        
        Key majorKeyToFind = Key.createKey(majorComponents);
        Object entity = null;
        Map<String, Object> relationMap = null;
        if(entityMetadata.getRelationNames() != null && ! entityMetadata.getRelationNames().isEmpty())
        {
            relationMap = new HashMap<String, Object>();
        }
        
        try 
        {            
            Iterator<KeyValueVersion> iterator = kvStore.multiGetIterator(Direction.FORWARD, 0, majorKeyToFind, null, null);

            if(! iterator.hasNext())
            {
                return null;
            } 
            else
            {
                entity = entityMetadata.getEntityClazz().newInstance();
                PropertyAccessorHelper.setId(entity, entityMetadata, key);
                
                while (iterator.hasNext()) {
                    KeyValueVersion keyValueVersion = iterator.next();               
                    
                    String minorKeyFirstPart = keyValueVersion.getKey().getMinorPath().get(0);
                    
                    if(superColumnNameToFieldMap.containsKey(minorKeyFirstPart))
                    {
                        Field embeddedField = superColumnNameToFieldMap.get(minorKeyFirstPart);
                        Class<?> embeddableClass = embeddedField.getType();
                        if (metamodel.isEmbeddable(embeddableClass))
                        {
                            String minorKeySecondPart = keyValueVersion.getKey().getMinorPath().get(1);
                            
                            Object embeddedObject = PropertyAccessorHelper.getObject(entity, embeddedField);
                            if(embeddedObject == null)
                            {
                                embeddedObject = embeddableClass.newInstance();
                                PropertyAccessorHelper.set(entity, embeddedField, embeddedObject);
                            }                          
                            
                            Field f = columnNameToFieldMap.get(minorKeySecondPart);           
                            if(f != null)
                            {
                                byte[] value = keyValueVersion.getValue().getValue();
                                PropertyAccessorHelper.set(embeddedObject, f, value);
                            }                     
                            
                        }                   
                        
                    }
                    else if(columnNameToFieldMap.containsKey(minorKeyFirstPart))
                    {
                        Value v = keyValueVersion.getValue();
                        Field f = columnNameToFieldMap.get(minorKeyFirstPart); 
                        
                        if(f != null && entityMetadata.getRelation(f.getName()) == null)
                        {
                            PropertyAccessorHelper.set(entity, f, v.getValue());
                        } 
                        else if (entityMetadata.getRelationNames() != null && entityMetadata.getRelationNames().contains(minorKeyFirstPart))
                        {      
                            Relation relation = entityMetadata.getRelation(f.getName());
                            EntityMetadata associationMetadata = KunderaMetadataManager
                                    .getEntityMetadata(relation.getTargetEntity());
                            relationMap.put(minorKeyFirstPart, PropertyAccessorHelper.getObject(associationMetadata.getIdAttribute()
                                    .getBindableJavaType(), v.getValue()));
                        }
                        
                        
                    }
                }
            }     
            
            
        } catch (Exception e) {
            log.error(e.getMessage());
            return null;
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
    public void close() {
        // TODO Once pool is implemented this code should not be there.
        // Workaround for pool
        getIndexManager().flush();

    }

    @Override
    public void delete(Object entity, Object pKey)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());

        List<String> majorKeyComponent = new ArrayList<String>();
        majorKeyComponent.add(entityMetadata.getTableName());
        majorKeyComponent.add(pKey.toString());

        
        kvStore.multiDelete(Key.createKey(majorKeyComponent),null,null);
        
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
        String schema = entityMetadata.getSchema();   //Irrelevant for this datastore
        String table = entityMetadata.getTableName();
        
        MetamodelImpl metamodel = (MetamodelImpl)KunderaMetadataManager.getMetamodel(entityMetadata.getPersistenceUnit());

        log.debug("Persisting data into " + schema + "." + table + " for " + id);     
        
        EntityType entityType = metamodel.entity(entityMetadata.getEntityClazz());
        
        //Major Key component
        List<String> majorKeyComponent = new ArrayList<String>();
        majorKeyComponent.add(table);
        majorKeyComponent.add(PropertyAccessorHelper.getString(id));    //Major keys are always String

        //Iterate over all Non-ID attributes of this entity (ID is already part of major key)
        Set<Attribute> attributes = entityType.getSingularAttributes();        
        
        for(Attribute attribute : attributes)
        {
            if (! attribute.equals(entityMetadata.getIdAttribute()))
            {
                Class fieldJavaType = ((AbstractAttribute) attribute).getBindableJavaType();               
                
                
                //If attribute is Embeddable, create minor keys for each attribute it contains
                if (metamodel.isEmbeddable(fieldJavaType))
                {
                    Object embeddedObject = PropertyAccessorHelper.getObject(entity, (Field) attribute.getJavaMember());
                    if(embeddedObject != null)
                    {
                        if (attribute.isCollection())
                        {
                            //ElementCollection is not supported for OracleNoSQL as of now, ignore for now
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
                                
                                //Key
                                Key key = Key.createKey(majorKeyComponent, minorKeyComponents);
                                
                                //Value
                                byte[] valueByteArray = PropertyAccessorHelper.get(embeddedObject, f);            
                                Value value = Value.createValue(valueByteArray);                                
                                kvStore.put(key, value);                                 
                            }               
                            
                        }
                    }  
                }
                
                //All other non-embeddable agttributes (ignore associations, as they will be store by separate call)
                else if (!attribute.isAssociation())
                {
                    Field field = (Field) attribute.getJavaMember();
                    String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                    
                    //Key
                    Key key = Key.createKey(majorKeyComponent, columnName);
                    
                    //Value
                    byte[] valueByteArray = PropertyAccessorHelper.get(entity, field);            
                    Value value = Value.createValue(valueByteArray);
                    
                    kvStore.put(key, value); 
                }               
            }           
        }
        
        //Iterate over relations
        if(rlHolders != null && ! rlHolders.isEmpty())
        {
            for(RelationHolder rh : rlHolders)
            {
                String relationName = rh.getRelationName();  
                Object valueObj = rh.getRelationValue();
                
                if(! StringUtils.isEmpty(relationName) && valueObj != null)
                {
                  //Key
                    Key key = Key.createKey(majorKeyComponent, relationName);
                    
                    //Value
                    byte[] valueInBytes = PropertyAccessorHelper.getBytes(valueObj);
                    Value value = Value.createValue(valueInBytes);
                    
                    kvStore.put(key, value);
                }   
                
            }
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
        return reader;
    }

    @Override
    public Class<OracleNoSQLQuery> getQueryImplementor()
    {
        return OracleNoSQLQuery.class;
    } 

}



