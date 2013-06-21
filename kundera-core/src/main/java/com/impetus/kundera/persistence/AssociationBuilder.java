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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.ObjectGraphUtils;
import com.impetus.kundera.index.IndexManager;
import com.impetus.kundera.lifecycle.states.ManagedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.JoinTableMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.context.MainCache;
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

    /*
     * <<<<<<< HEAD
     *//**
     * Populates entities related via join table for <code>entity</code>
     * 
     * @param entity
     * @param entityMetadata
     * @param delegator
     * @param relation
     */
    /*
     * void populateRelationForM2M(Object entity, EntityMetadata entityMetadata,
     * PersistenceDelegator delegator, Relation relation, Object relObject,
     * Map<String, Object> relationsMap) { // For M-M relationship of Collection
     * type, relationship entities are // always fetched from Join Table. if
     * (relation.getPropertyType().isAssignableFrom(List.class) ||
     * relation.getPropertyType().isAssignableFrom(Set.class)) { if
     * (relation.isRelatedViaJoinTable() && (relObject == null ||
     * PersistenceUtilHelper.instanceOfHibernateProxy(relObject) ||
     * PersistenceUtilHelper.instanceOfHibernatePersistentSet(relObject) ||
     * PersistenceUtilHelper
     * .instanceOfHibernatePersistentCollection(relObject))) {
     * populateCollectionFromJoinTable(entity, entityMetadata, delegator,
     * relation); }
     * 
     * } else if (relation.getPropertyType().isAssignableFrom(Map.class)) { if
     * (relation.isRelatedViaJoinTable()) { // TODO: Implement Map relationships
     * via Join Table (not // supported as of now) } else {
     * populateCollectionFromMap(entity, delegator, relation, relObject,
     * relationsMap); } }
     * 
     * }
     */
    /*    *//**
     * @param entity
     * @param pd
     * @param relation
     * @param relationValue
     */
    /*
     * void populateRelationFromValue(Object entity, PersistenceDelegator pd,
     * Relation relation, Object relationValue, EntityMetadata childMetadata) {
     * Class<?> childClass = relation.getTargetEntity();
     * 
     * Object child = pd.find(childClass, relationValue); child = child != null
     * && child instanceof EnhanceEntity ? ((EnhanceEntity) child).getEntity() :
     * child;
     * 
     * if (child != null) { PropertyAccessorHelper.set(entity,
     * relation.getProperty(), child);
     * 
     * // If child has any bidirectional relationship, process them here Field
     * biDirectionalField = relation.getBiDirectionalField(); boolean
     * isBidirectionalRelation = relation.isBiDirectional();
     * 
     * if (isBidirectionalRelation) { Relation reverseRelation =
     * childMetadata.getRelation(biDirectionalField.getName());
     * 
     * if (relation.getType().equals(ForeignKey.ONE_TO_ONE)) {
     * PropertyAccessorHelper.set(child, reverseRelation.getProperty(), entity);
     * } else { Object childId = PropertyAccessorHelper.getId(child,
     * childMetadata); EntityMetadata reverseEntityMetadata =
     * KunderaMetadataManager.getEntityMetadata(entity.getClass());
     * populateRelationViaQuery(child, pd, childId, reverseRelation,
     * relation.getJoinColumnName(), reverseEntityMetadata); }
     * 
     * } } }
     */
    /*  *//**
     * @param entity
     * @param pd
     * @param entityId
     * @param relation
     * @param relationName
     */
    /*
     * void populateRelationViaQuery(Object entity, PersistenceDelegator pd,
     * Object entityId, Relation relation, String relationName, EntityMetadata
     * childMetadata) { Class<?> childClass = relation.getTargetEntity(); Client
     * childClient = pd.getClient(childMetadata);
     * 
     * List associatedObjects = null;
     * 
     * // Since ID is stored at other side of the relationship, we have to //
     * query that table if
     * (MetadataUtils.useSecondryIndex(childClient.getPersistenceUnit())) { //
     * Pass this entity id as a value to be searched for associatedObjects =
     * pd.find(childClass, entityId, relationName); } else { associatedObjects =
     * getAssociatedEntitiesFromIndex(entity.getClass(), entityId, childClass,
     * childClient); }
     * 
     * List associatedEntities = new ArrayList(); if (associatedObjects != null
     * && !associatedObjects.isEmpty()) { for (Object o : associatedObjects) {
     * if (o instanceof EnhanceEntity) { associatedEntities.add(((EnhanceEntity)
     * o).getEntity()); } else { associatedEntities.add(o); } }
     * setAssociatedEntities(entity, relation.getProperty(),
     * associatedEntities); }
     * 
     * // If child has any bidirectional relationship, process them here Field
     * biDirectionalField = relation.getBiDirectionalField();
     * 
     * boolean traversalRequired = true; boolean isBidirectionalRelation =
     * relation.isBiDirectional();
     * 
     * if (isBidirectionalRelation && associatedEntities != null) { Relation
     * reverseRelation =
     * childMetadata.getRelation(biDirectionalField.getName());
     * 
     * for (Object child : associatedEntities) { // String childId =
     * PropertyAccessorHelper.getId(child, // childMetadata); // EntityMetadata
     * reverseEntityMetadata = //
     * KunderaMetadataManager.getEntityMetadata(entity.getClass());
     * 
     * // populateRelationFromValue(child, pd, reverseRelation, // entityId,
     * childMetadata); // reverseRelation.getProperty().getType() List
     * biDirectionEntities = new ArrayList(1); biDirectionEntities.add(entity);
     * 
     * setAssociatedEntities(child, reverseRelation.getProperty(),
     * biDirectionEntities);
     * 
     * // PropertyAccessorHelper.set(child, // reverseRelation.getProperty(),
     * entity); }
     * 
     * traversalRequired =
     * (reverseRelation.getType().equals(ForeignKey.ONE_TO_ONE) && ! StringUtils
     * . isBlank ( reverseRelation . getMappedBy ( ) ) ) ||
     * (reverseRelation.getType().equals(ForeignKey.MANY_TO_ONE) && !
     * StringUtils . isBlank ( reverseRelation . getMappedBy ( ) ) );
     * 
     * }
     * 
     * if (associatedEntities != null) { // Save children entities to
     * persistence cache MainCache mainCache = (MainCache)
     * pd.getPersistenceCache().getMainCache();
     * 
     * for (Object child : associatedEntities) { Object childId =
     * PropertyAccessorHelper.getId(child, childMetadata);
     * 
     * String nodeId = ObjectGraphUtils.getNodeId(childId,
     * childMetadata.getEntityClazz()); Node node = new Node(nodeId,
     * childMetadata.getEntityClazz(), new ManagedState(),
     * pd.getPersistenceCache(), childId); node.setData(child);
     * node.setPersistenceDelegator(pd); mainCache.addNodeToCache(node); } }
     * 
     * // Recursively find associated entities if
     * ((childMetadata.getRelationNames() == null ||
     * childMetadata.getRelationNames().isEmpty()) &&
     * !childMetadata.isRelationViaJoinTable()) { // There is no relation (not
     * even via Join Table), nothing to do if (log.isDebugEnabled())
     * log.info("Nothing to do, simply moving to next:"); }
     * 
     * else if (traversalRequired && associatedEntities != null) { // These
     * entities has associated entities, find them recursively. for (Object
     * associatedEntity : associatedEntities) { associatedEntity =
     * childClient.getReader().recursivelyFindEntities(associatedEntity, null,
     * childMetadata, pd); } }
     * 
     * }
     */
    /*    *//**
     * Populates a relationship of type {@link Collection} (i.e. those of
     * type {@link Set} or {@link List})
     */
    /*
     * private void populateCollectionFromJoinTable(Object entity,
     * EntityMetadata entityMetadata, PersistenceDelegator delegator, Relation
     * relation) { JoinTableMetadata jtMetadata =
     * relation.getJoinTableMetadata(); Client pClient =
     * delegator.getClient(entityMetadata);
     * 
     * String schema = entityMetadata.getSchema();
     * 
     * EntityMetadata owningEntityMetadata =
     * KunderaMetadataManager.getEntityMetadata(relation.getTargetEntity());
     * Class columnJavaType =
     * owningEntityMetadata.getIdAttribute().getJavaType(); if (jtMetadata ==
     * null) { columnJavaType = entityMetadata.getIdAttribute().getJavaType();
     * jtMetadata =
     * owningEntityMetadata.getRelation(relation.getMappedBy()).getJoinTableMetadata
     * (); pClient = delegator.getClient(owningEntityMetadata); schema =
     * owningEntityMetadata.getSchema(); }
     * 
     * String joinTableName = jtMetadata.getJoinTableName();
     * 
     * Set<String> joinColumns = jtMetadata.getJoinColumns(); Set<String>
     * inverseJoinColumns = jtMetadata.getInverseJoinColumns();
     * 
     * String joinColumnName = (String) joinColumns.toArray()[0]; String
     * inverseJoinColumnName = (String) inverseJoinColumns.toArray()[0];
     * 
     * // EntityMetadata relMetadata = //
     * delegator.getMetadata(relation.getTargetEntity());
     * 
     * Object entityId = PropertyAccessorHelper.getId(entity, entityMetadata);
     * List<?> foreignKeys = pClient.getColumnsById(schema, joinTableName,
     * joinColumnName, inverseJoinColumnName, entityId, columnJavaType);
     * 
     * List childrenEntities = new ArrayList(); for (Object foreignKey :
     * foreignKeys) { EntityMetadata childMetadata =
     * KunderaMetadataManager.getEntityMetadata(relation.getTargetEntity());
     * 
     * Object child = delegator.find(relation.getTargetEntity(), foreignKey);
     * Object obj = child instanceof EnhanceEntity && child != null ?
     * ((EnhanceEntity) child).getEntity() : child;
     * 
     * // If child has any bidirectional relationship, process them here Field
     * biDirectionalField = relation.getBiDirectionalField(); boolean
     * isBidirectionalRelation = relation.isBiDirectional();
     * 
     * if (isBidirectionalRelation && obj != null) { Object columnValue =
     * PropertyAccessorHelper.getId(obj, childMetadata); Object[] pKeys =
     * pClient.findIdsByColumn(entityMetadata.getSchema(), joinTableName,
     * joinColumnName, inverseJoinColumnName, columnValue,
     * entityMetadata.getEntityClazz()); List parents =
     * delegator.find(entity.getClass(), pKeys); PropertyAccessorHelper.set(obj,
     * biDirectionalField, ObjectUtils.getFieldInstance(parents,
     * biDirectionalField)); }
     * 
     * childrenEntities.add(obj); }
     * 
     * Field childField = relation.getProperty();
     * 
     * try { PropertyAccessorHelper.set( entity, childField,
     * PropertyAccessorHelper.isCollection(childField.getType()) ?
     * ObjectUtils.getFieldInstance( childrenEntities, childField) :
     * childrenEntities.get(0));
     * PersistenceCacheManager.addEntityToPersistenceCache(entity, delegator,
     * entityId); } catch (PropertyAccessException ex) { throw new
     * EntityReaderException(ex); } }
     *//**
     * Populates a a relationship collection which is of type {@link Map}
     * from relationsMap into entity
     * 
     * @param entity
     * @param delegator
     * @param relation
     * @param relObject
     * @param relationsMap
     */
    /*
     * private void populateCollectionFromMap(Object entity,
     * PersistenceDelegator delegator, Relation relation, Object relObject,
     * Map<String, Object> relationsMap) { EntityMetadata childMetadata =
     * KunderaMetadataManager.getEntityMetadata(relation.getTargetEntity());
     * Map<Object, Object> relationshipEntityMap = new HashMap<Object,
     * Object>(); // Map // collection // to // be // set // into // entity
     * 
     * if (relObject == null && relationsMap != null && !relationsMap.isEmpty())
     * { for (String relationName : relationsMap.keySet()) { Object
     * relationValue = relationsMap.get(relationName); if (relationValue
     * instanceof Map) { Map<Object, Object> relationValueMap = (Map<Object,
     * Object>) relationValue;
     * 
     * Client targetEntityClient = delegator.getClient(childMetadata); // Client
     * // for // target // entity for (Object targetEntityKey :
     * relationValueMap.keySet()) { // Find target entity from database Object
     * targetEntity = targetEntityClient.find(childMetadata.getEntityClazz(),
     * targetEntityKey); if (targetEntity != null && targetEntity instanceof
     * EnhanceEntity) { targetEntity = ((EnhanceEntity)
     * targetEntity).getEntity(); }
     * 
     * // Set source and target entities into Map key entity Object mapKeyEntity
     * = relationValueMap.get(targetEntityKey); Class<?> relationshipClass =
     * relation.getMapKeyJoinClass(); for (Field f :
     * relationshipClass.getDeclaredFields()) { if
     * (f.getType().equals(entity.getClass())) {
     * PropertyAccessorHelper.set(mapKeyEntity, f, entity); } else if
     * (f.getType().equals(childMetadata.getEntityClazz())) {
     * PropertyAccessorHelper.set(mapKeyEntity, f, targetEntity); } }
     * 
     * // Finally, put map key and value into collection
     * relationshipEntityMap.put(mapKeyEntity, targetEntity); } } } relObject =
     * relationshipEntityMap; }
     * 
     * // Set relationship collection into original entity
     * PropertyAccessorHelper.set(entity, relation.getProperty(), relObject);
     * 
     * // Add target entities into persistence cache if (relObject != null) {
     * for (Object child : ((Map) relObject).values()) { if (child != null) {
     * Object childId = PropertyAccessorHelper.getId(child, childMetadata);
     * PersistenceCacheManager.addEntityToPersistenceCache(child, delegator,
     * childId); } } } }
     *//**
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

    /*    *//**
     * Returns associated bi-directional field.
     * 
     * @param originalClazz
     *            Original class
     * @param referencedClass
     *            Referenced class.
     */
    /*
     * public Field getBiDirectionalField(Class originalClazz, Class
     * referencedClass) { Field[] fields = referencedClass.getDeclaredFields();
     * Class<?> clazzz = null; Field biDirectionalField = null; for (Field field
     * : fields) { clazzz = field.getType(); if
     * (PropertyAccessorHelper.isCollection(clazzz)) { ParameterizedType type =
     * (ParameterizedType) field.getGenericType(); Type[] types =
     * type.getActualTypeArguments(); clazzz = (Class<?>) types[0]; } else if
     * (Map.class.isAssignableFrom(clazzz)) { ParameterizedType type =
     * (ParameterizedType) field.getGenericType(); Type[] types =
     * type.getActualTypeArguments(); clazzz = (Class<?>) types[1]; } if
     * (clazzz.equals(originalClazz)) { biDirectionalField = field; break; } }
     * return biDirectionalField; }
     */
    /* *//**
     * Sets associated entities to <code>entity</code>
     * 
     * @param entity
     * @param f
     * @param associatedEntities
     * @return
     * @throws PropertyAccessException
     */
    /*
     * private Set<?> setAssociatedEntities(Object entity, Field f, List<?>
     * associatedEntities) throws PropertyAccessException { Set chids = new
     * HashSet(); if (associatedEntities != null) { chids = new
     * HashSet(associatedEntities); PropertyAccessorHelper.set( entity, f,
     * PropertyAccessorHelper.isCollection(f.getType()) ?
     * ObjectUtils.getFieldInstance(associatedEntities, f) :
     * associatedEntities.get(0)); } return chids; }
     */

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
     * @param entity
     * @param pd
     * @param relation
     * @param relationValue
     */
    void populateRelationFromValue(Object entity, PersistenceDelegator pd, Relation relation, Object relationValue,
            EntityMetadata childMetadata)
    {
        Class<?> childClass = relation.getTargetEntity();

        Object child = pd.find(childClass, relationValue);
        child = child != null && child instanceof EnhanceEntity ? ((EnhanceEntity) child).getEntity() : child;

        if (child != null)
        {
            PropertyAccessorHelper.set(entity, relation.getProperty(), child);

            // If child has any bidirectional relationship, process them here
            Field biDirectionalField = getBiDirectionalField(entity.getClass(), relation.getTargetEntity());
            boolean isBidirectionalRelation = (biDirectionalField != null);

            if (isBidirectionalRelation)
            {
                Relation reverseRelation = childMetadata.getRelation(biDirectionalField.getName());

                if (relation.getType().equals(ForeignKey.ONE_TO_ONE))
                {
                    PropertyAccessorHelper.set(child, reverseRelation.getProperty(), entity);
                }
                else
                {
                    Object childId = PropertyAccessorHelper.getId(child, childMetadata);
                    EntityMetadata reverseEntityMetadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
                    populateRelationViaQuery(child, pd, childId, reverseRelation, relation.getJoinColumnName(),
                            reverseEntityMetadata);
                }

            }
        }
    }

    /**
     * @param entity
     * @param pd
     * @param entityId
     * @param relation
     * @param relationName
     */
    void populateRelationViaQuery(Object entity, PersistenceDelegator pd, Object entityId, Relation relation,
            String relationName, EntityMetadata childMetadata)
    {
        Class<?> childClass = relation.getTargetEntity();
        Client childClient = pd.getClient(childMetadata);

        List associatedObjects = null;

        // Since ID is stored at other side of the relationship, we have to
        // query that table
        if (MetadataUtils.useSecondryIndex(childClient.getPersistenceUnit()))
        {
            // Pass this entity id as a value to be searched for
            associatedObjects = pd.find(childClass, entityId, relationName);
        }
        else
        {
            associatedObjects = getAssociatedEntitiesFromIndex(entity, entityId, childClass, childClient);
        }

        List associatedEntities = new ArrayList();
        if (associatedObjects != null && !associatedObjects.isEmpty())
        {
            for (Object o : associatedObjects)
            {
                if (o instanceof EnhanceEntity)
                {
                    associatedEntities.add(((EnhanceEntity) o).getEntity());
                }
                else
                {
                    associatedEntities.add(o);
                }
            }
            setAssociatedEntities(entity, relation.getProperty(), associatedEntities);
        }

        // If child has any bidirectional relationship, process them here
        Field biDirectionalField = getBiDirectionalField(entity.getClass(), relation.getTargetEntity());

        boolean traversalRequired = true;
        boolean isBidirectionalRelation = (biDirectionalField != null);

        if (isBidirectionalRelation && associatedEntities != null)
        {
            Relation reverseRelation = childMetadata.getRelation(biDirectionalField.getName());

            for (Object child : associatedEntities)
            {
                // String childId = PropertyAccessorHelper.getId(child,
                // childMetadata);
                // EntityMetadata reverseEntityMetadata =
                // KunderaMetadataManager.getEntityMetadata(entity.getClass());

                // populateRelationFromValue(child, pd, reverseRelation,
                // entityId, childMetadata);
                // reverseRelation.getProperty().getType()
                List biDirectionEntities = new ArrayList(1);
                biDirectionEntities.add(entity);

                setAssociatedEntities(child, reverseRelation.getProperty(), biDirectionEntities);

                // PropertyAccessorHelper.set(child,
                // reverseRelation.getProperty(), entity);
            }

            traversalRequired = (reverseRelation.getType().equals(ForeignKey.ONE_TO_ONE) /*
                                                                                          * &&
                                                                                          * !
                                                                                          * StringUtils
                                                                                          * .
                                                                                          * isBlank
                                                                                          * (
                                                                                          * reverseRelation
                                                                                          * .
                                                                                          * getMappedBy
                                                                                          * (
                                                                                          * )
                                                                                          * )
                                                                                          */)
                    || (reverseRelation.getType().equals(ForeignKey.MANY_TO_ONE) /*
                                                                                  * &&
                                                                                  * !
                                                                                  * StringUtils
                                                                                  * .
                                                                                  * isBlank
                                                                                  * (
                                                                                  * reverseRelation
                                                                                  * .
                                                                                  * getMappedBy
                                                                                  * (
                                                                                  * )
                                                                                  * )
                                                                                  */);

        }

        if (associatedEntities != null)
        {
            // Save children entities to persistence cache
            MainCache mainCache = (MainCache) pd.getPersistenceCache().getMainCache();

            for (Object child : associatedEntities)
            {
                Object childId = PropertyAccessorHelper.getId(child, childMetadata);

                String nodeId = ObjectGraphUtils.getNodeId(childId, childMetadata.getEntityClazz());
                Node node = new Node(nodeId, childMetadata.getEntityClazz(), new ManagedState(),
                        pd.getPersistenceCache(), childId);
                node.setData(child);
                node.setPersistenceDelegator(pd);
                mainCache.addNodeToCache(node);
            }
        }

        // Recursively find associated entities
        if ((childMetadata.getRelationNames() == null || childMetadata.getRelationNames().isEmpty())
                && !childMetadata.isRelationViaJoinTable())
        {
            // There is no relation (not even via Join Table), nothing to do
            if (log.isDebugEnabled())
                log.info("Nothing to do, simply moving to next:");
        }

        else if (traversalRequired && associatedEntities != null)
        {
            // These entities has associated entities, find them recursively.
            for (Object associatedEntity : associatedEntities)
            {
                associatedEntity = childClient.getReader().recursivelyFindEntities(associatedEntity, null,
                        childMetadata, pd);
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

        EntityMetadata owningEntityMetadata = KunderaMetadataManager.getEntityMetadata(relation.getTargetEntity());
        Class columnJavaType = owningEntityMetadata.getIdAttribute().getJavaType();
        if (jtMetadata == null)
        {
            columnJavaType = entityMetadata.getIdAttribute().getJavaType();
            jtMetadata = owningEntityMetadata.getRelation(relation.getMappedBy()).getJoinTableMetadata();
            pClient = delegator.getClient(owningEntityMetadata);
            schema = owningEntityMetadata.getSchema();
        }

        String joinTableName = jtMetadata.getJoinTableName();

        Set<String> joinColumns = jtMetadata.getJoinColumns();
        Set<String> inverseJoinColumns = jtMetadata.getInverseJoinColumns();

        String joinColumnName = (String) joinColumns.toArray()[0];
        String inverseJoinColumnName = (String) inverseJoinColumns.toArray()[0];

        // EntityMetadata relMetadata =
        // delegator.getMetadata(relation.getTargetEntity());

        Object entityId = PropertyAccessorHelper.getId(entity, entityMetadata);
        List<?> foreignKeys = pClient.getColumnsById(schema, joinTableName, joinColumnName, inverseJoinColumnName,
                entityId, columnJavaType);

        List childrenEntities = new ArrayList();
        for (Object foreignKey : foreignKeys)
        {
            EntityMetadata childMetadata = KunderaMetadataManager.getEntityMetadata(relation.getTargetEntity());

            Object child = delegator.find(relation.getTargetEntity(), foreignKey);
            Object obj = child instanceof EnhanceEntity && child != null ? ((EnhanceEntity) child).getEntity() : child;

            // If child has any bidirectional relationship, process them here
            Field biDirectionalField = getBiDirectionalField(entity.getClass(), relation.getTargetEntity());
            boolean isBidirectionalRelation = (biDirectionalField != null);

            if (isBidirectionalRelation && obj != null)
            {
                Object columnValue = PropertyAccessorHelper.getId(obj, childMetadata);
                Object[] pKeys = pClient.findIdsByColumn(entityMetadata.getSchema(), joinTableName, joinColumnName,
                        inverseJoinColumnName, columnValue, entityMetadata.getEntityClazz());
                List parents = delegator.find(entity.getClass(), pKeys);
                PropertyAccessorHelper.set(obj, biDirectionalField,
                        ObjectUtils.getFieldInstance(parents, biDirectionalField));
            }

            childrenEntities.add(obj);
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
        EntityMetadata childMetadata = KunderaMetadataManager.getEntityMetadata(relation.getTargetEntity());
        Map<Object, Object> relationshipEntityMap = new HashMap<Object, Object>(); // Map
                                                                                   // collection
                                                                                   // to
                                                                                   // be
                                                                                   // set
                                                                                   // into
                                                                                   // entity

        if (relObject == null && relationsMap != null && !relationsMap.isEmpty())
        {
            for (String relationName : relationsMap.keySet())
            {
                Object relationValue = relationsMap.get(relationName);
                if (relationValue instanceof Map)
                {
                    Map<Object, Object> relationValueMap = (Map<Object, Object>) relationValue;

                    Client targetEntityClient = delegator.getClient(childMetadata); // Client
                                                                                    // for
                                                                                    // target
                                                                                    // entity
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
        if (relObject != null)
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
    private List getAssociatedEntitiesFromIndex(Object entity, Object entityId, Class<?> childClass, Client childClient)
    {

        List associatedEntities;
        IndexManager indexManager = childClient.getIndexManager();

        Map<String, Object> results = indexManager != null ? indexManager.search(entity.getClass(), childClass,
                entityId) : new HashMap<String, Object>();
        Set rsSet = results != null ? new HashSet(results.values()) : new HashSet();

        if (childClass.equals(entity.getClass()))
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
     * Returns associated bi-directional field.
     * 
     * @param originalClazz
     *            Original class
     * @param referencedClass
     *            Referenced class.
     */
    public Field getBiDirectionalField(Class originalClazz, Class referencedClass)
    {
        Field[] fields = referencedClass.getDeclaredFields();
        Class<?> clazzz = null;
        Field biDirectionalField = null;
        for (Field field : fields)
        {
            clazzz = field.getType();
            if (PropertyAccessorHelper.isCollection(clazzz))
            {
                ParameterizedType type = (ParameterizedType) field.getGenericType();
                Type[] types = type.getActualTypeArguments();
                clazzz = (Class<?>) types[0];
            }
            else if (Map.class.isAssignableFrom(clazzz))
            {
                ParameterizedType type = (ParameterizedType) field.getGenericType();
                Type[] types = type.getActualTypeArguments();
                clazzz = (Class<?>) types[1];
            }
            if (clazzz.equals(originalClazz))
            {
                biDirectionalField = field;
                break;
            }
        }
        return biDirectionalField;
    }

    /**
     * Sets associated entities to <code>entity</code>
     * 
     * @param entity
     * @param f
     * @param associatedEntities
     * @return
     * @throws PropertyAccessException
     */
    private Set<?> setAssociatedEntities(Object entity, Field f, List<?> associatedEntities)
            throws PropertyAccessException
    {
        Set chids = new HashSet();
        if (associatedEntities != null)
        {
            chids = new HashSet(associatedEntities);
            PropertyAccessorHelper.set(
                    entity,
                    f,
                    PropertyAccessorHelper.isCollection(f.getType()) ? ObjectUtils.getFieldInstance(associatedEntities,
                            f) : associatedEntities.get(0));
        }
        return chids;
    }

    /**
     * @param relationsMap
     * @param type
     * @return
     */
    private boolean isTraversalRequired(Map<String, Object> relationsMap, ForeignKey type)
    {
        return !(relationsMap == null && (type.equals(ForeignKey.ONE_TO_ONE) || type.equals(ForeignKey.MANY_TO_ONE)));
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
        String relationName = MetadataUtils.getMappedName(m, relation);
        Object relationValue = relationsMap != null ? relationsMap.get(relationName) : null;

        if ((relation.getType().equals(ForeignKey.ONE_TO_ONE) || relation.getType().equals(ForeignKey.MANY_TO_ONE)))
        { // One-To-One or
          // Many-To-One
          // relationship

            Field biDirectionalField = getBiDirectionalField(entity.getClass(), relation.getTargetEntity());
            boolean isBidirectionalRelation = (biDirectionalField != null);
            if (isBidirectionalRelation && relationValue == null)
            {
                EntityMetadata parentEntityMetadata = KunderaMetadataManager.getEntityMetadata(relation
                        .getTargetEntity());
                Object owner = null;
                KunderaProxy kp = KunderaMetadata.INSTANCE.getCoreMetadata().getLazyInitializerFactory().getProxy();
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
                        String entityName = m.getEntityClazz().getName() + "_" + entityId + "#"
                                + relation.getProperty().getName();

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

    /**
     * @param entity
     * @param relationsMap
     * @param m
     * @param pd
     * @param entityId
     * @param relation
     */
    public void setConcreteRelationObject(Object entity, Map<String, Object> relationsMap, EntityMetadata m,
            PersistenceDelegator pd, Object entityId, Relation relation)
    {
        if (isTraversalRequired(relationsMap, relation.getType()))
        {
            // Check whether that relation is already populated or not,
            // before proceeding further.
            Object object = PropertyAccessorHelper.getObject(entity, relation.getProperty());

            // Populate Many-to-many relationships
            if (relation.getType().equals(ForeignKey.MANY_TO_MANY))
            {
                // First, Save this entity to persistence cache
                PersistenceCacheManager.addEntityToPersistenceCache(entity, pd, entityId);
                populateRelationForM2M(entity, m, pd, relation, object, relationsMap);
            }

            // Populate other type of relationships
            else if (object == null || ProxyHelper.isProxyOrCollection(object))
            {
                Field biDirectionalField = getBiDirectionalField(relation.getTargetEntity(), entity.getClass());
                boolean isBidirectionalRelation = (biDirectionalField != null);

                Class<?> childClass = relation.getTargetEntity();
                EntityMetadata childMetadata = KunderaMetadataManager.getEntityMetadata(childClass);

                Object relationValue = null;
                String relationName = null;

                if (isBidirectionalRelation
                        && !StringUtils.isBlank(relation.getMappedBy())
                        && (relation.getType().equals(ForeignKey.ONE_TO_ONE) || relation.getType().equals(
                                ForeignKey.MANY_TO_ONE)))
                {
                    relationName = ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName();
                }
                else
                {
                    relationName = MetadataUtils.getMappedName(m, relation);
                }

                relationValue = relationsMap != null ? relationsMap.get(relationName) : null;

                if (relationValue != null)
                {
                    // 1-1 or M-1 relationship, because ID is held at
                    // this side of entity and hence
                    // relationship entities would be retrieved from
                    // database based on these IDs already available
                    populateRelationFromValue(entity, pd, relation, relationValue, childMetadata);

                }
                else
                {
                    // 1-M relationship, since ID is stored at other
                    // side of
                    // entity and as a result relation value will be
                    // null
                    // This requires running query (either Lucene or
                    // Native
                    // based on secondary indexes supported by
                    // underlying
                    // database)
                    // Running query returns all those associated
                    // entities
                    // that
                    // hold parent entity ID as foreign key
                    populateRelationViaQuery(entity, pd, entityId, relation, relationName, childMetadata);
                }
            }

        }
        else if (relation.isJoinedByPrimaryKey())
        {
            PropertyAccessorHelper.set(entity, relation.getProperty(),
                    pd.findById(relation.getTargetEntity(), entityId));
        }
    }

    private KunderaProxy getLazyEntity(String entityName, Class<?> persistentClass, Method getIdentifierMethod,
            Method setIdentifierMethod, Object id, PersistenceDelegator pd)
    {
        return KunderaMetadata.INSTANCE.getCoreMetadata().getLazyInitializerFactory()
                .getProxy(entityName, persistentClass, getIdentifierMethod, setIdentifierMethod, id, pd);
    }

}
