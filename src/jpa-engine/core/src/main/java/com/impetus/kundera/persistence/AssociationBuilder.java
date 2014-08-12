/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.persistence;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.JoinTableMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.context.PersistenceCacheManager;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.KunderaProxy;
import com.impetus.kundera.proxy.ProxyHelper;
import com.impetus.kundera.proxy.collection.ProxyCollection;
import com.impetus.kundera.proxy.collection.ProxyList;
import com.impetus.kundera.proxy.collection.ProxyMap;
import com.impetus.kundera.proxy.collection.ProxySet;
import com.impetus.kundera.utils.ObjectUtils;

/**
 * This class is responsible for building association for given entities.
 * 
 * @author vivek.mishra
 */
public final class AssociationBuilder
{

    private static Logger log = LoggerFactory.getLogger(AssociationBuilder.class);

    /**
     * Retrieves associated entities from secondary index. There are two
     * alternatives here:
     * 
     * 1. Via running Lucene query into Lucene powered secondary index. 2.
     * Searching into a secondary index by custom secondary index class provided
     * by user.
     * 
     * @see PersistenceProperties#KUNDERA_INDEX_HOME_DIR
     * @see PersistenceProperties#KUNDERA_INDEXER_CLASS
     * 
     *      TODO: Which secondary index to use should be transparent. All we
     *      should bother about is indexer.index(), indexer.search() etc.
     */
    List getAssociatedEntitiesFromIndex(Class owningClazz, Object entityId, Class<?> childClass, Client childClient)
    {
        List associatedEntities;
        IndexManager indexManager = childClient.getIndexManager();

        Map<String, Object> results = indexManager != null ? indexManager.search(owningClazz, childClass, entityId)
                : new HashMap<String, Object>();
        Set rsSet = results != null ? new HashSet(results.values()) : new HashSet();

        if (childClass.equals(owningClazz))
        {
            associatedEntities = (List<Object>) childClient.findAll(childClass, null, rsSet.toArray(new Object[] {}));
        }
        else
        {
            associatedEntities = (List<Object>) childClient.findAll(childClass, null, rsSet.toArray(new Object[] {}));
        }
        return associatedEntities;
    }

    /**
     * Populates entities related via join table for <code>entity</code>
     * 
     * @param entity
     * @param entityMetadata
     * @param delegator
     * @param relation
     */
    void populateRelationForM2M(Object entity, EntityMetadata entityMetadata, PersistenceDelegator delegator,
            Relation relation, Object relObject, Map<String, Object> relationsMap)
    {
        // For M-M relationship of Collection type, relationship entities are
        // always fetched from Join Table.
        if (relation.getPropertyType().isAssignableFrom(List.class)
                || relation.getPropertyType().isAssignableFrom(Set.class))
        {
            if (relation.isRelatedViaJoinTable() && (relObject == null || ProxyHelper.isProxyOrCollection(relObject)))
            {
                populateCollectionFromJoinTable(entity, entityMetadata, delegator, relation);
            }

        }
        else if (relation.getPropertyType().isAssignableFrom(Map.class))
        {
            if (relation.isRelatedViaJoinTable())
            {
                // TODO: Implement Map relationships via Join Table (not
                // supported as of now)
            }
            else
            {
                populateCollectionFromMap(entity, delegator, relation, relObject, relationsMap);
            }
        }
    }

    /**
     * Populates a relationship of type {@link Collection} (i.e. those of type
     * {@link Set} or {@link List})
     */
    private void populateCollectionFromJoinTable(Object entity, EntityMetadata entityMetadata,
            PersistenceDelegator delegator, Relation relation)
    {
        JoinTableMetadata jtMetadata = relation.getJoinTableMetadata();
        Client pClient = delegator.getClient(entityMetadata);

        String schema = entityMetadata.getSchema();

        EntityMetadata owningEntityMetadata = KunderaMetadataManager.getEntityMetadata(delegator.getKunderaMetadata(),
                relation.getTargetEntity());
        Class columnJavaType = owningEntityMetadata.getIdAttribute().getJavaType();
        if (jtMetadata == null)
        {
            columnJavaType = entityMetadata.getIdAttribute().getJavaType();
            Relation owningEntityMetadataRelation = owningEntityMetadata.getRelation(relation.getMappedBy());
            jtMetadata = owningEntityMetadataRelation.getJoinTableMetadata();
            pClient = delegator.getClient(owningEntityMetadata);
            schema = owningEntityMetadata.getSchema();
        }

        String joinTableName = jtMetadata.getJoinTableName();

        Set<String> joinColumns = jtMetadata.getJoinColumns();
        Set<String> inverseJoinColumns = jtMetadata.getInverseJoinColumns();

        String joinColumnName = (String) joinColumns.toArray()[0];
        String inverseJoinColumnName = (String) inverseJoinColumns.toArray()[0];

        Object entityId = PropertyAccessorHelper.getId(entity, entityMetadata);
        List<?> foreignKeys = pClient.getColumnsById(schema, joinTableName, joinColumnName, inverseJoinColumnName,
                entityId, columnJavaType);

        List childrenEntities = new ArrayList();

        if (foreignKeys != null)
        {
            for (Object foreignKey : foreignKeys)
            {
                EntityMetadata childMetadata = KunderaMetadataManager.getEntityMetadata(delegator.getKunderaMetadata(),
                        relation.getTargetEntity());

                Object child = delegator.find(relation.getTargetEntity(), foreignKey);
                Object obj = child instanceof EnhanceEntity && child != null ? ((EnhanceEntity) child).getEntity()
                        : child;

                // If child has any bidirectional relationship, process them
                // here
                Field biDirectionalField = relation.getBiDirectionalField();
                boolean isBidirectionalRelation = (biDirectionalField != null);

                if (isBidirectionalRelation && obj != null)
                {
                    Object columnValue = PropertyAccessorHelper.getId(obj, childMetadata);
                    Object[] pKeys = pClient.findIdsByColumn(entityMetadata.getSchema(), joinTableName, joinColumnName,
                            inverseJoinColumnName, columnValue, entityMetadata.getEntityClazz());
                    List parents = delegator.find(entity.getClass(), pKeys);
                    PropertyAccessorHelper.set(obj, biDirectionalField,
                            ObjectUtils.getFieldInstance(parents, biDirectionalField));
                    PersistenceCacheManager.addEntityToPersistenceCache(obj, delegator, columnValue);
                }

                childrenEntities.add(obj);
            }
        }
        Field childField = relation.getProperty();

        try
        {
            PropertyAccessorHelper.set(
                    entity,
                    childField,
                    PropertyAccessorHelper.isCollection(childField.getType()) ? ObjectUtils.getFieldInstance(
                            childrenEntities, childField) : childrenEntities.get(0));
            PersistenceCacheManager.addEntityToPersistenceCache(entity, delegator, entityId);
        }
        catch (PropertyAccessException ex)
        {
            throw new EntityReaderException(ex);
        }
    }

    /**
     * Populates a a relationship collection which is of type {@link Map} from
     * relationsMap into entity
     * 
     * @param entity
     * @param delegator
     * @param relation
     * @param relObject
     * @param relationsMap
     */
    private void populateCollectionFromMap(Object entity, PersistenceDelegator delegator, Relation relation,
            Object relObject, Map<String, Object> relationsMap)
    {
        EntityMetadata childMetadata = KunderaMetadataManager.getEntityMetadata(delegator.getKunderaMetadata(),
                relation.getTargetEntity());
        // Map collection to be set into entity
        Map<Object, Object> relationshipEntityMap = new HashMap<Object, Object>();

        if ((relObject == null || ProxyHelper.isProxyCollection(relObject)) && relationsMap != null
                && !relationsMap.isEmpty())
        {
            for (String relationName : relationsMap.keySet())
            {
                Object relationValue = relationsMap.get(relationName);
                if (relationValue instanceof Map)
                {
                    Map<Object, Object> relationValueMap = (Map<Object, Object>) relationValue;

                    // Client for target entity
                    Client targetEntityClient = delegator.getClient(childMetadata);

                    for (Object targetEntityKey : relationValueMap.keySet())
                    {
                        // Find target entity from database
                        Object targetEntity = targetEntityClient.find(childMetadata.getEntityClazz(), targetEntityKey);
                        if (targetEntity != null && targetEntity instanceof EnhanceEntity)
                        {
                            targetEntity = ((EnhanceEntity) targetEntity).getEntity();
                        }

                        // Set source and target entities into Map key entity
                        Object mapKeyEntity = relationValueMap.get(targetEntityKey);
                        Class<?> relationshipClass = relation.getMapKeyJoinClass();
                        for (Field f : relationshipClass.getDeclaredFields())
                        {
                            if (f.getType().equals(entity.getClass()))
                            {
                                PropertyAccessorHelper.set(mapKeyEntity, f, entity);
                            }
                            else if (f.getType().equals(childMetadata.getEntityClazz()))
                            {
                                PropertyAccessorHelper.set(mapKeyEntity, f, targetEntity);
                            }
                        }

                        // Finally, put map key and value into collection
                        relationshipEntityMap.put(mapKeyEntity, targetEntity);
                    }
                }
            }
            relObject = relationshipEntityMap;
        }

        // Set relationship collection into original entity
        PropertyAccessorHelper.set(entity, relation.getProperty(), relObject);

        // Add target entities into persistence cache
        if (relObject != null && !ProxyHelper.isProxyCollection(relObject))
        {
            for (Object child : ((Map) relObject).values())
            {
                if (child != null)
                {
                    Object childId = PropertyAccessorHelper.getId(child, childMetadata);
                    PersistenceCacheManager.addEntityToPersistenceCache(child, delegator, childId);
                }
            }
        }
    }

    /**
     * @param entity
     * @param relationsMap
     * @param m
     * @param pd
     * @param entityId
     * @param relation
     */
    public void setProxyRelationObject(Object entity, Map<String, Object> relationsMap, EntityMetadata m,
            PersistenceDelegator pd, Object entityId, Relation relation)
    {
        KunderaMetadata kunderaMetadata = pd.getKunderaMetadata();
        String relationName = MetadataUtils.getMappedName(m, relation, kunderaMetadata);
        Object relationValue = relationsMap != null ? relationsMap.get(relationName) : null;

        if ((relation.getType().equals(ForeignKey.ONE_TO_ONE) || relation.getType().equals(ForeignKey.MANY_TO_ONE)))
        { // One-To-One or Many-To-One relationship

            Field biDirectionalField = relation.getBiDirectionalField();
            boolean isBidirectionalRelation = (biDirectionalField != null);
            if (isBidirectionalRelation && (relationValue == null && !relation.isJoinedByPrimaryKey()))
            {
                EntityMetadata parentEntityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                        relation.getTargetEntity());
                Object owner = null;

                String entityName = m.getEntityClazz().getName() + "_" + entityId + "#"
                        + relation.getProperty().getName();

                KunderaProxy kp = kunderaMetadata.getCoreMetadata().getLazyInitializerFactory().getProxy(entityName);

                if (kp != null)
                {
                    owner = kp.getKunderaLazyInitializer().getOwner();
                    if (owner != null && owner.getClass().equals(parentEntityMetadata.getEntityClazz()))
                    {

                        relationValue = PropertyAccessorHelper.getId(owner, parentEntityMetadata);
                    }

                    if (relationValue != null)
                    {
                        if (log.isDebugEnabled())
                        {
                            log.debug("Creating proxy for >> " + parentEntityMetadata.getEntityClazz().getName() + "#"
                                    + relation.getProperty().getName() + "_" + relationValue);
                        }

                        Object proxy = getLazyEntity(entityName, relation.getTargetEntity(),
                                parentEntityMetadata.getReadIdentifierMethod(),
                                parentEntityMetadata.getWriteIdentifierMethod(), relationValue, pd);
                        PropertyAccessorHelper.set(entity, relation.getProperty(), proxy);
                    }
                }

            }

            else if (relationValue != null)
            {
                if (log.isDebugEnabled())
                {
                    log.debug("Creating proxy for >> " + m.getEntityClazz().getName() + "#"
                            + relation.getProperty().getName() + "_" + relationValue);
                }

                String entityName = m.getEntityClazz().getName() + "_" + entityId + "#"
                        + relation.getProperty().getName();

                Object proxy = getLazyEntity(entityName, relation.getTargetEntity(), m.getReadIdentifierMethod(),
                        m.getWriteIdentifierMethod(), relationValue, pd);
                PropertyAccessorHelper.set(entity, relation.getProperty(), proxy);

            }
            else if (relation.isJoinedByPrimaryKey())
            {
                if (log.isDebugEnabled())
                {
                    log.debug("Creating proxy for >> " + m.getEntityClazz().getName() + "#"
                            + relation.getProperty().getName() + "_" + relationValue);
                }

                String entityName = m.getEntityClazz().getName() + "_" + entityId + "#"
                        + relation.getProperty().getName();

                Object proxy = getLazyEntity(entityName, relation.getTargetEntity(), m.getReadIdentifierMethod(),
                        m.getWriteIdentifierMethod(), entityId, pd);
                PropertyAccessorHelper.set(entity, relation.getProperty(), proxy);
            }

        }
        else if (relation.getType().equals(ForeignKey.ONE_TO_MANY)
                || relation.getType().equals(ForeignKey.MANY_TO_MANY))
        {
            ProxyCollection proxyCollection = null;

            if (relation.getPropertyType().isAssignableFrom(Set.class))
            {
                proxyCollection = new ProxySet(pd, relation);

            }
            else if (relation.getPropertyType().isAssignableFrom(List.class))
            {
                proxyCollection = new ProxyList(pd, relation);
            }

            else if (relation.getPropertyType().isAssignableFrom(Map.class))
            {
                proxyCollection = new ProxyMap(pd, relation);
            }

            proxyCollection.setOwner(entity);
            proxyCollection.setRelationsMap(relationsMap);

            PropertyAccessorHelper.set(entity, relation.getProperty(), proxyCollection);
        }
    }

    private KunderaProxy getLazyEntity(String entityName, Class<?> persistentClass, Method getIdentifierMethod,
            Method setIdentifierMethod, Object id, PersistenceDelegator pd)
    {
        return pd.getKunderaMetadata().getCoreMetadata().getLazyInitializerFactory()
                .getProxy(entityName, persistentClass, getIdentifierMethod, setIdentifierMethod, id, pd);
    }

}
