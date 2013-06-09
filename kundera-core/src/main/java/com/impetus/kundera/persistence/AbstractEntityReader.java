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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.impetus.kundera.PersistenceUtilHelper;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.hibernate.collection.internal.PersistentSet;
//import org.hibernate.collection.spi.PersistentCollection;
//import org.hibernate.proxy.HibernateProxy;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
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

    AssociationBuilder associationBuilder;

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
    public Object recursivelyFindEntities(Object entity, Map<String, Object> relationsMap, EntityMetadata m,
            PersistenceDelegator pd)
    {
        Object entityId = PropertyAccessorHelper.getId(entity, m);
        associationBuilder = new AssociationBuilder();

        for (Relation relation : m.getRelations())
        {
            // validate relation
            ForeignKey type = relation.getType();
            Field f = relation.getProperty();
            if (isTraversalRequired(relationsMap, type))
            {
                // Check whether that relation is already populated or not,
                // before proceeding further.
                Object object = PropertyAccessorHelper.getObject(entity, f);

                // Populate Many-to-many relationships
                if (relation.getType().equals(ForeignKey.MANY_TO_MANY))
                {
                    // First, Save this entity to persistence cache
                    PersistenceCacheManager.addEntityToPersistenceCache(entity, pd, entityId);
                    associationBuilder.populateRelationForM2M(entity, m, pd, relation, object, relationsMap);
                }

                // Populate other type of relationships
                else if (object == null || PersistenceUtilHelper.instanceOfHibernateProxy( object ) || PersistenceUtilHelper.instanceOfHibernatePersistentSet( object )
                        || PersistenceUtilHelper.instanceOfHibernatePersistentCollection( object ))
                {
                    Field biDirectionalField = associationBuilder.getBiDirectionalField(relation.getTargetEntity(),
                            entity.getClass());
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
                        associationBuilder
                                .populateRelationFromValue(entity, pd, relation, relationValue, childMetadata);

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
                        associationBuilder.populateRelationViaQuery(entity, pd, entityId, relation, relationName,
                                childMetadata);
                    }
                }

            }
            else if (relation.isJoinedByPrimaryKey())
            {
                PropertyAccessorHelper.set(entity, f, pd.findById(relation.getTargetEntity(), entityId));
            }
        }
        return entity;
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
}