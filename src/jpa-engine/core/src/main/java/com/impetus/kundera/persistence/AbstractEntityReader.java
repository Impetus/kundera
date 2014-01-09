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

import javax.persistence.FetchType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.persistence.context.PersistenceCacheManager;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.proxy.ProxyHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;

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
    private Object handleAssociation(final Object entity, final Map<String, Object> relationsMap,
            final EntityMetadata m, final PersistenceDelegator pd, boolean lazilyloaded)
    {

        for (Relation relation : m.getRelations())
        {
            ForeignKey relationType = relation.getType();

            Object relationalObject = PropertyAccessorHelper.getObject(entity, relation.getProperty());

            if (KunderaCoreUtils.isEmptyOrNull(relationalObject) || ProxyHelper.isProxyOrCollection(relationalObject))
            {
                onRelation(entity, relationsMap, m, pd, relation, relationType, lazilyloaded);
            }
        }
        return entity;
    }

    /**
     * Parse over each relation of fetched entity.
     * 
     * @param entity
     * @param relationsMap
     * @param m
     * @param pd
     * @param relation
     * @param relationType
     * @param lazilyloaded
     */
    private void onRelation(final Object entity, final Map<String, Object> relationsMap, final EntityMetadata m,
            final PersistenceDelegator pd, Relation relation, ForeignKey relationType, boolean lazilyloaded)
    {

        FetchType fetchType = relation.getFetchType();

        if (!lazilyloaded && fetchType.equals(FetchType.LAZY))
        {
            final Object entityId = PropertyAccessorHelper.getId(entity, m);
            getAssociationBuilder().setProxyRelationObject(entity, relationsMap, m, pd, entityId, relation);
        }
        else
        {
            if (relation.getType().equals(ForeignKey.MANY_TO_MANY))
            {
                // First, Save this entity to persistence cache
                Field f = relation.getProperty();
                Object object = PropertyAccessorHelper.getObject(entity, f);
                final Object entityId = PropertyAccessorHelper.getId(entity, m);
                PersistenceCacheManager.addEntityToPersistenceCache(entity, pd, entityId);
                getAssociationBuilder().populateRelationForM2M(entity, m, pd, relation, object, relationsMap);
            }
            else
            {
                onRelation(entity, relationsMap, relation, m, pd, lazilyloaded);
            }
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
            final EntityMetadata metadata, final PersistenceDelegator pd, boolean lazilyloaded)
    {
        final Object entityId = PropertyAccessorHelper.getId(entity, metadata);

        // if relation map contains value, then invoke target entity with find
        // by id.
        // else invoke target entity for find by relation, pass it's entityId as
        // a column value and relation.getJoinColumnName as column name.

        Object relationValue = relationsMap != null ? relationsMap.get(relation.getJoinColumnName()) : null;
        EntityMetadata targetEntityMetadata = KunderaMetadataManager.getEntityMetadata(relation.getTargetEntity());

        List relationalEntities = fetchRelations(relation, metadata, pd, entityId, relationValue, targetEntityMetadata);

        // parse for associated relation.

        if (relationalEntities != null)
        {
            for (Object relationEntity : relationalEntities)
            {
                if (relationEntity != null)
                {
                    onParseRelation(entity, pd, targetEntityMetadata, relationEntity, relation, lazilyloaded);
                    PersistenceCacheManager.addEntityToPersistenceCache(getEntity(relationEntity), pd,
                            PropertyAccessorHelper.getId(relationEntity, targetEntityMetadata));
                }
            }
        }

    }

    /**
     * Invokes parseRelations for relation entity and set relational entity
     * within entity
     * 
     * @param entity
     * @param pd
     * @param targetEntityMetadata
     * @param relationEntity
     * @param relation
     * @param lazilyloaded
     */
    private void onParseRelation(Object entity, final PersistenceDelegator pd, EntityMetadata targetEntityMetadata,
            Object relationEntity, Relation relation, boolean lazilyloaded)
    {
        parseRelations(entity, getEntity(relationEntity), getPersistedRelations(relationEntity), pd,
                targetEntityMetadata, lazilyloaded);

        // if relation ship is unary, no problem else we need to add
        setRelationToEntity(entity, relationEntity, relation);
    }

    /**
     * After successfully parsing set relational entity object within entity
     * object.
     * 
     * @param entity
     * @param relationEntity
     * @param relation
     */
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
                if (associationObject == null || ProxyHelper.isProxyOrCollection(associationObject))
                {
                    associationObject = PropertyAccessorHelper.getCollectionInstance(relation.getProperty());
                    PropertyAccessorHelper.set(entity, relation.getProperty(), associationObject);
                }

                ((Collection) associationObject).add(getEntity(relationEntity));
            }
        }
    }

    /**
     * Parse relations of provided relationEntity.
     * 
     * @param originalEntity
     * @param relationEntity
     * @param relationsMap
     * @param pd
     * @param metadata
     * @param lazilyloaded
     */
    private void parseRelations(final Object originalEntity, final Object relationEntity,
            final Map<String, Object> relationsMap, final PersistenceDelegator pd, final EntityMetadata metadata,
            boolean lazilyloaded)
    {

        for (Relation relation : metadata.getRelations())
        {

            FetchType fetchType = relation.getFetchType();

            if (!lazilyloaded && fetchType.equals(FetchType.LAZY))
            {
                final Object entityId = PropertyAccessorHelper.getId(relationEntity, metadata);
                getAssociationBuilder().setProxyRelationObject(relationEntity, relationsMap, metadata, pd, entityId,
                        relation);
            }
            else
            {

                if (relation.isUnary() && relation.getTargetEntity().isAssignableFrom(originalEntity.getClass()))
                {
                    Object associationObject = PropertyAccessorHelper.getObject(relationEntity, relation.getProperty());
                    if (relation.getType().equals(ForeignKey.ONE_TO_ONE))
                    {
                        if ((associationObject == null || ProxyHelper.isProxyOrCollection(associationObject)))
                        {
                            PropertyAccessorHelper.set(relationEntity, relation.getProperty(), originalEntity);
                        }
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
                    // Here in case of one-to-many/many-to-one we should skip
                    // this
                    // relation as it
                    if (immediateRelations != null && !immediateRelations.isEmpty())
                    {
                        // As it
                        // is
                        // already
                        // in process.

                        for (Object immediateRelation : immediateRelations)
                        {
                            if (!compareTo(getEntity(immediateRelation), originalEntity))
                            {
                                onParseRelation(relationEntity, pd, targetEntityMetadata, immediateRelation, relation,
                                        lazilyloaded);
                            }
                        }
                        setRelationToEntity(relationEntity, originalEntity, relation);
                        PersistenceCacheManager.addEntityToPersistenceCache(getEntity(relationEntity), pd,
                                PropertyAccessorHelper.getId(relationEntity, metadata));
                    }
                }
            }
        }
    }

    /**
     * 
     * Based on relation type, method invokes database to fetch relation
     * entities.
     * 
     * @param relation
     *            relation
     * @param metadata
     *            entity metadata
     * @param pd
     *            persistence delegator
     * @param entityId
     *            entity id
     * @param relationValue
     *            relational value
     * @param targetEntityMetadata
     *            relational entity's metadata.
     * 
     * @return list of fetched relations.
     */
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

            if (!MetadataUtils.useSecondryIndex(((ClientBase) associatedClient).getClientMetadata())
                    && MetadataUtils.indexSearchEnabled(targetEntityMetadata.getPersistenceUnit()))
            {

                relationalEntities = getAssociationBuilder().getAssociatedEntitiesFromIndex(
                        relation.getProperty().getDeclaringClass(), entityId, targetEntityMetadata.getEntityClazz(),
                        associatedClient);
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
            PersistenceDelegator pd, boolean lazilyLoaded)
    {
        return handleAssociation(entity, relationsMap, m, pd, lazilyLoaded);

    }

    /**
     * Returns wrapped relations.
     * 
     * @param relationEntity
     * @return
     */
    private Map<String, Object> getPersistedRelations(Object relationEntity)
    {
        return relationEntity != null && relationEntity.getClass().isAssignableFrom(EnhanceEntity.class) ? ((EnhanceEntity) relationEntity)
                .getRelations() : null;
    }

    /**
     * Returns wrapped entity.
     * 
     * @param relationEntity
     * @return
     */
    private Object getEntity(Object relationEntity)
    {
        return relationEntity != null && relationEntity.getClass().isAssignableFrom(EnhanceEntity.class) ? ((EnhanceEntity) relationEntity)
                .getEntity() : relationEntity;
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
        Set<String> rSet = fetchDataFromLucene(m.getEntityClazz(), client);
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
    protected Set<String> fetchDataFromLucene(Class<?> clazz, Client client)
    {
        // use lucene to query and get Pk's only.
        // go to client and get relation with values.!
        // populate EnhanceEntity
        Map<String, Object> results = client.getIndexManager().search(clazz, luceneQueryFromJPAQuery);
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

    /**
     * Compares original with relational entity.
     * 
     * @param relationalEntity
     * @param originalEntity
     * @return
     */
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

    /**
     * Returns association builder instance.
     * 
     * @return association builder
     */
    private AssociationBuilder getAssociationBuilder()
    {
        if (this.associationBuilder == null)
        {
            this.associationBuilder = new AssociationBuilder();
        }

        return this.associationBuilder;

    }
}