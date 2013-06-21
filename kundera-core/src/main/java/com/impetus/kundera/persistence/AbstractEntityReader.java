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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceUtilHelper;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.persistence.context.PersistenceCacheManager;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * The Class AbstractEntityReader.
 * 
 * @author vivek.mishra
 */
public class AbstractEntityReader
{

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(AbstractEntityReader.class);

    /** The lucene query from jpa query. */
    protected String luceneQueryFromJPAQuery;

    private AssociationBuilder associationBuilder;

    /**
     * Retrieves an entity from ID
     * 
     * @param primaryKey
     * @param m
     * @param client
     * @return
     */
    protected EnhanceEntity findById(Object primaryKey, EntityMetadata m, Client client)
    {
        try
        {
            Object o = client.find(m.getEntityClazz(), primaryKey);

            if (o == null)
            {
                // No entity found
                return null;
            }
            else
            {
                return o instanceof EnhanceEntity ? (EnhanceEntity) o : new EnhanceEntity(o, getId(o, m), null);
            }
        }
        catch (Exception e)
        {
            throw new EntityReaderException(e);
        }
    }

    /**
     * Recursively fetches associated entities for a given <code>entity</code>
     * 
     * @param entity
     * @param relationsMap
     * @param client
     * @param m
     * @param pd
     * @return
     */
    public Object handleAssociation(final Object entity, final Map<String, Object> relationsMap,
            final EntityMetadata m, final PersistenceDelegator pd)
    {

        for (Relation relation : m.getRelations())
        {
            ForeignKey relationType = relation.getType();

            Object relationalObject = PropertyAccessorHelper.getObject(entity, relation.getProperty());
            
            //TODO: Need to check if object is a collection instance but empty!
            if (relationalObject == null || PersistenceUtilHelper.instanceOfHibernateProxy(relationalObject)
                    || PersistenceUtilHelper.instanceOfHibernatePersistentSet(relationalObject)
                    || PersistenceUtilHelper.instanceOfHibernatePersistentCollection(relationalObject)
                    || PersistenceUtilHelper.instanceOfHibernatePersistentBag(relationalObject))
            {
                onRelation(entity, relationsMap, m, pd, relation, relationType);
            }
        }
        return entity;
    }

    private void onRelation(final Object entity, final Map<String, Object> relationsMap, final EntityMetadata m,
            final PersistenceDelegator pd, Relation relation, ForeignKey relationType)
    {
        // TODO: MANY TO MANY
        if (relation.getType().equals(ForeignKey.MANY_TO_MANY))
        {
            // First, Save this entity to persistence cache
            Field f = relation.getProperty();
            Object object = PropertyAccessorHelper.getObject(entity, f);
            final Object entityId = PropertyAccessorHelper.getId(entity, m);
            PersistenceCacheManager.addEntityToPersistenceCache(entity, pd, entityId);
            associationBuilder.populateRelationForM2M(entity, m, pd, relation, object, relationsMap);
        }
        else
        {
            onRelation(entity, relationsMap, relation, m, pd);
        }

    }

    /**
     * Method to handle one to one association relation events.
     * 
     * @param entity
     *            relation owning entity.
     * @param entityId
     *            entity id of relation owning entity.
     * @param relationsMap
     *            contains relation name and it's value.
     * @param m
     *            entity metadata.
     */
    private void onRelation(Object entity, Map<String, Object> relationsMap, final Relation relation,
            final EntityMetadata metadata, final PersistenceDelegator pd)
    {
        final Object entityId = PropertyAccessorHelper.getId(entity, metadata);

        // if relation map contains value, then invoke target entity with find
        // by id.
        // else invoke target entity for find by relation, pass it's entityId as
        // a column value and relation.getJoinColumnName as column name.

        Object relationValue = relationsMap != null ? relationsMap.get(relation.getJoinColumnName()) : null;
        // EntityMetadata targetEntityMetadata = null;
        EntityMetadata targetEntityMetadata = KunderaMetadataManager.getEntityMetadata(relation.getTargetEntity());

        List relationalEntities = fetchRelations(relation, metadata, pd, entityId, relationValue, targetEntityMetadata);

        // parse for associated relation.

        if (relationalEntities != null)
        {
            for (Object relationEntity : relationalEntities)
            {
                onParseRelation(entity, pd, targetEntityMetadata, relationEntity, relation);
            }
        }

    }

    private void onParseRelation(Object entity, final PersistenceDelegator pd, EntityMetadata targetEntityMetadata,
            Object relationEntity, Relation relation)
    {
        parseRelations(entity, getEntity(relationEntity), getPersistedRelations(relationEntity), pd,
                targetEntityMetadata);

        // if relation ship is unary, no problem else we need to add
        setRelationToEntity(entity, relationEntity, relation);
    }

    private void setRelationToEntity(Object entity, Object relationEntity, Relation relation)
    {
        if (relation.getTargetEntity().isAssignableFrom(getEntity(relationEntity).getClass()))
        {

            if (relation.isUnary())
            {
                PropertyAccessorHelper.set(entity, relation.getProperty(), getEntity(relationEntity));
            }
            else
            {
                Object associationObject = PropertyAccessorHelper.getObject(entity, relation.getProperty());
                if (associationObject == null || PersistenceUtilHelper.instanceOfHibernateProxy(associationObject)
                        || PersistenceUtilHelper.instanceOfHibernatePersistentSet(associationObject)
                        || PersistenceUtilHelper.instanceOfHibernatePersistentCollection(associationObject)
                        || PersistenceUtilHelper.instanceOfHibernatePersistentBag(associationObject))
                {
                    associationObject = PropertyAccessorHelper.getCollectionInstance(relation.getProperty());
                    PropertyAccessorHelper.set(entity, relation.getProperty(), associationObject);
                }

                ((Collection) associationObject).add(getEntity(relationEntity));
            }
        }
    }

    private void parseRelations(final Object originalEntity, final Object relationEntity,
            final Map<String, Object> relationsMap, final PersistenceDelegator pd, final EntityMetadata metadata)
    {

        for (Relation relation : metadata.getRelations())
        {
            if (relation.isUnary() && relation.getTargetEntity().isAssignableFrom(originalEntity.getClass()))
            {
                // PropertyAccessorHelper.set(relationEntity,
                // relation.getProperty(), originalEntity);

                Object associationObject = PropertyAccessorHelper.getObject(relationEntity, relation.getProperty());
                if (relation.getType().equals(ForeignKey.ONE_TO_ONE)
                        || ((associationObject == null
                                || PersistenceUtilHelper.instanceOfHibernateProxy(associationObject)
                                || PersistenceUtilHelper.instanceOfHibernatePersistentSet(associationObject) || PersistenceUtilHelper
                                    .instanceOfHibernatePersistentCollection(associationObject))))
                {
                    PropertyAccessorHelper.set(relationEntity, relation.getProperty(), originalEntity);
                }
                else if (relationsMap != null && relationsMap.containsKey(relation.getJoinColumnName()))
                {
                    PropertyAccessorHelper.set(relationEntity, relation.getProperty(), originalEntity);
                }
            }
            else
            {
                // Here
                // onRelation(relationEntity, relationsMap, metadata, pd,
                // relation, relationType);
                final Object entityId = PropertyAccessorHelper.getId(relationEntity, metadata);
                Object relationValue = relationsMap != null ? relationsMap.get(relation.getJoinColumnName()) : null;
                final EntityMetadata targetEntityMetadata = KunderaMetadataManager.getEntityMetadata(relation
                        .getTargetEntity());
                List immediateRelations = fetchRelations(relation, metadata, pd, entityId, relationValue,
                        targetEntityMetadata);
                // System.out.println("dddd");
                // Here in case of one-to-many/many-to-one we should skip this
                // relation as it
                if (immediateRelations != null && !immediateRelations.isEmpty())
                {
                    // immediateRelations.remove(originalEntity); // As it is
                    // already
                    // in process.

                    for (Object immediateRelation : immediateRelations)
                    {
                        // System.out.println("Here");
                        if (!compareTo(getEntity(immediateRelation), originalEntity))
                        {
                            onParseRelation(relationEntity, pd, targetEntityMetadata, immediateRelation, relation);
                        }
                    }

                    setRelationToEntity(relationEntity, originalEntity, relation);
                }

                /**
                 * 
                 */

            }
        }

    }

    private List fetchRelations(final Relation relation, final EntityMetadata metadata, final PersistenceDelegator pd,
            final Object entityId, Object relationValue, EntityMetadata targetEntityMetadata)
    {
        List relationalEntities = new ArrayList();

        if ((relationValue != null && relation.isUnary()) || (relation.isJoinedByPrimaryKey()))
        {
            // Call it
            Object relationEntity = pd.getClient(targetEntityMetadata).find(relation.getTargetEntity(),
                    relationValue != null ? relationValue : entityId);
            if (relationEntity != null)
            {
                relationalEntities.add(relationEntity);
            }
        }
        else if (!relation.isUnary())
        {
            // Now these entities may be enhance entities and may not be as
            // well.
            Client associatedClient = pd.getClient(targetEntityMetadata);
            if (!MetadataUtils.useSecondryIndex(targetEntityMetadata.getPersistenceUnit()))
            {

                
                relationalEntities = associationBuilder.getAssociatedEntitiesFromIndex(relation.getProperty().getDeclaringClass(),
                        entityId, targetEntityMetadata.getEntityClazz(), associatedClient);
            }
            else
            {

                relationalEntities = associatedClient.findByRelation(relation.getJoinColumnName(), entityId,
                        relation.getTargetEntity());
            }
        }
        return relationalEntities;
    }

    /**
     * Recursively fetches associated entities for a given <code>entity</code>
     * 
     * @param entity
     * @param relationsMap
     * @param client
     * @param m
     * @param pd
     * @return
     */
    public Object recursivelyFindEntities(Object entity, Map<String, Object> relationsMap, EntityMetadata m,
            PersistenceDelegator pd)
    {
        associationBuilder = new AssociationBuilder();
        /*
         * Object entityId = PropertyAccessorHelper.getId(entity, m);
         * associationBuilder = new AssociationBuilder();
         * 
         * for (Relation relation : m.getRelations()) { // validate relation
         * ForeignKey type = relation.getType(); Field f =
         * relation.getProperty(); if (isTraversalRequired(relationsMap, type))
         * { // Check whether that relation is already populated or not, //
         * before proceeding further. Object object =
         * PropertyAccessorHelper.getObject(entity, f);
         * 
         * // Populate Many-to-many relationships if
         * (relation.getType().equals(ForeignKey.MANY_TO_MANY)) { // First, Save
         * this entity to persistence cache
         * PersistenceCacheManager.addEntityToPersistenceCache(entity, pd,
         * entityId); associationBuilder.populateRelationForM2M(entity, m, pd,
         * relation, object, relationsMap); }
         * 
         * // Populate other type of relationships else if (object == null ||
         * PersistenceUtilHelper.instanceOfHibernateProxy(object) ||
         * PersistenceUtilHelper.instanceOfHibernatePersistentSet(object) ||
         * PersistenceUtilHelper
         * .instanceOfHibernatePersistentCollection(object)) { boolean
         * isBidirectionalRelation = relation.isBiDirectional();
         * 
         * Class<?> childClass = relation.getTargetEntity(); EntityMetadata
         * childMetadata = KunderaMetadataManager.getEntityMetadata(childClass);
         * 
         * Object relationValue = null; String relationName = null;
         * 
         * if (isBidirectionalRelation &&
         * !StringUtils.isBlank(relation.getMappedBy()) &&
         * (relation.getType().equals(ForeignKey.ONE_TO_ONE) ||
         * relation.getType().equals( ForeignKey.MANY_TO_ONE))) { relationName =
         * ((AbstractAttribute) m.getIdAttribute()).getJPAColumnName(); } else {
         * relationName = MetadataUtils.getMappedName(m, relation); }
         * 
         * relationValue = relationsMap != null ? relationsMap.get(relationName)
         * : null;
         * 
         * if (relationValue != null) { // 1-1 or M-1 relationship, because ID
         * is held at // this side of entity and hence // relationship entities
         * would be retrieved from // database based on these IDs already
         * available associationBuilder .populateRelationFromValue(entity, pd,
         * relation, relationValue, childMetadata);
         * 
         * } else { // 1-M relationship, since ID is stored at other // side of
         * // entity and as a result relation value will be // null // This
         * requires running query (either Lucene or // Native // based on
         * secondary indexes supported by // underlying // database) // Running
         * query returns all those associated // entities // that // hold parent
         * entity ID as foreign key
         * associationBuilder.populateRelationViaQuery(entity, pd, entityId,
         * relation, relationName, childMetadata); } }
         * 
         * } else if (relation.isJoinedByPrimaryKey()) {
         * PropertyAccessorHelper.set(entity, f,
         * pd.findById(relation.getTargetEntity(), entityId)); } } return
         * entity;
         */
        return handleAssociation(entity, relationsMap, m, pd);

    }

    private Map<String, Object> getPersistedRelations(Object relationEntity)
    {
        return relationEntity != null && relationEntity.getClass().isAssignableFrom(EnhanceEntity.class) ? ((EnhanceEntity) relationEntity)
                .getRelations() : null;
    }

    private Object getEntity(Object relationEntity)
    {
        return relationEntity.getClass().isAssignableFrom(EnhanceEntity.class) ? ((EnhanceEntity) relationEntity)
                .getEntity() : relationEntity;
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
     * On association using lucene.
     * 
     * @param m
     *            the m
     * @param client
     *            the client
     * @param ls
     *            the ls
     * @return the list
     */
    protected List<EnhanceEntity> onAssociationUsingLucene(EntityMetadata m, Client client, List<EnhanceEntity> ls)
    {
        Set<String> rSet = fetchDataFromLucene(client);
        List resultList = client.findAll(m.getEntityClazz(), null, rSet.toArray(new String[] {}));
        return m.getRelationNames() != null && !m.getRelationNames().isEmpty() ? resultList : transform(m, ls,
                resultList);
    }

    /**
     * Transform.
     * 
     * @param m
     *            the m
     * @param ls
     *            the ls
     * @param resultList
     *            the result list
     * @return the list
     */
    protected List<EnhanceEntity> transform(EntityMetadata m, List<EnhanceEntity> ls, List resultList)
    {
        if ((ls == null || ls.isEmpty()) && resultList != null && !resultList.isEmpty())
        {
            ls = new ArrayList<EnhanceEntity>(resultList.size());
        }
        for (Object r : resultList)
        {
            EnhanceEntity e = new EnhanceEntity(r, getId(r, m), null);
            ls.add(e);
        }
        return ls;
    }

    /**
     * Fetch data from lucene.
     * 
     * @param client
     *            the client
     * @return the sets the
     */
    protected Set<String> fetchDataFromLucene(Client client)
    {
        // use lucene to query and get Pk's only.
        // go to client and get relation with values.!
        // populate EnhanceEntity
        Map<String, Object> results = client.getIndexManager().search(luceneQueryFromJPAQuery);
        Set rSet = new HashSet(results.values());
        return rSet;
    }

    /**
     * Gets the id.
     * 
     * @param entity
     *            the entity
     * @param metadata
     *            the metadata
     * @return the id
     */
    protected Object getId(Object entity, EntityMetadata metadata)
    {
        try
        {
            return PropertyAccessorHelper.getId(entity, metadata);
        }
        catch (PropertyAccessException e)
        {
            log.error("Error while Getting ID, Caused by: ", e);
            throw new EntityReaderException("Error while Getting ID for entity " + entity, e);
        }

    }

    private boolean compareTo(Object relationalEntity, Object originalEntity)
    {
        if (relationalEntity != null && originalEntity != null
                && relationalEntity.getClass().isAssignableFrom(originalEntity.getClass()))
        {
            EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(originalEntity.getClass());

            Object relationalEntityId = PropertyAccessorHelper.getId(relationalEntity, metadata);
            Object originalEntityId = PropertyAccessorHelper.getId(originalEntity, metadata);

            return relationalEntityId.equals(originalEntityId);
        }

        return false;
    }
}