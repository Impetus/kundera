/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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

import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.index.DocumentIndexer;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.JoinTableMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.exception.QueryHandlerException;

/**
 * The Class AbstractEntityReader.
 * 
 * @author vivek.mishra
 */
public class AbstractEntityReader
{

    /** The log. */
    private static Log log = LogFactory.getLog(AbstractEntityReader.class);

    /** The lucene query from jpa query. */
    protected String luceneQueryFromJPAQuery;

    /**
     * Compute graph.
     * 
     * @param e
     *            the e
     * @param graphs
     *            the graphs
     * @param collectionHolder
     *            the collection holder
     * @param client
     *            the client
     * @param m
     *            the m
     * @param persistenceDelegeator
     *            the persistence delegeator
     * @return the object
     * @throws Exception
     *             the exception
     */
    public Object computeGraph(EnhanceEntity e, List<EntitySaveGraph> graphs, Map<Object, Object> collectionHolder,
            Client client, EntityMetadata m, PersistenceDelegator persistenceDelegeator) throws Exception
    {

        Client childClient = null;
        Class<?> childClazz = null;
        EntityMetadata childMetadata = null;
        // means it is holding associations
        // read graph for all the association
        for (EntitySaveGraph g : graphs)
        {
            Relation relation = m.getRelation(g.getProperty().getName());
            if (relation.isRelatedViaJoinTable())
            {
                computeJoinTableRelations(e, client, m, g, persistenceDelegeator, relation);

            }
            else
            {

                if (e.getEntity().getClass().equals(g.getChildClass()))
                {
                    // Swapped relationship
                    String relationName = g.getfKeyName();
                    Object relationalValue = e.getRelations().get(relationName);
                    childClazz = g.getParentClass();
                    childMetadata = persistenceDelegeator.getMetadata(childClazz);

                    Field f = g.getProperty();

                    if (!collectionHolder.containsKey(relationalValue))
                    {

                        childClient = persistenceDelegeator.getClient(childMetadata);

                        // Object child = childClient.find(childClazz,
                        // childMetadata, relationalValue.toString(), null);
                        Object child = null;

                        if (childClazz.equals(e.getEntity().getClass()))
                        {
                            child = childClient.find(childClazz, childMetadata, relationalValue.toString(), null);
                        }
                        else
                        {
                            child = persistenceDelegeator.find(childClazz, relationalValue.toString(), g);
                        }

                        collectionHolder.put(relationalValue, child);

                        // If entity is holding association it means it can not
                        // be a
                        // collection.
                    }

                    onBiDirection(e, client, g, m, collectionHolder.get(relationalValue), childMetadata, childClient);

                    List<Object> collection = new ArrayList<Object>(1);
                    collection.add(collectionHolder.get(relationalValue));
                    PropertyAccessorHelper.set(e.getEntity(), f,
                            PropertyAccessorHelper.isCollection(f.getType()) ? getFieldInstance(collection, f)
                                    : collection.get(0));
                }
                else
                {
                    childClazz = g.getChildClass();
                    childMetadata = persistenceDelegeator.getMetadata(childClazz);

                    childClient = persistenceDelegeator.getClient(childMetadata);
                    String relationName = g.getfKeyName();
                    String relationalValue = e.getEntityId();
                    Field f = g.getProperty();
                    if (!collectionHolder.containsKey(relationalValue))
                    {
                        // create a finder and pass metadata, relationName,
                        // relationalValue.
                        List<Object> childs = null;
                        if (MetadataUtils.useSecondryIndex(childClient.getPersistenceUnit()))
                        {
                            childs = childClient.find(relationName, relationalValue, childMetadata);

                            // pass this entity id as a value to be searched
                            // for
                            // for
                        }
                        else
                        {
                            if (g.isSharedPrimaryKey())
                            {
                                childs = new ArrayList();
                                // childs.add(childClient.find(childClazz,
                                // childMetadata, e.getEntityId(), null));
                                childs.add(childClazz.equals(e.getEntity().getClass()) ? childs.add(childClient.find(
                                        childClazz, childMetadata, e.getEntityId(), null)) : persistenceDelegeator
                                        .find(childClazz, relationalValue.toString()));
                            }
                            else
                            {
                                // lucene query, where entity class is child
                                // class, parent class is entity's class and
                                // parentid is entity ID!
                                // that's it!
                                String query = getQuery(DocumentIndexer.PARENT_ID_CLASS, e.getEntity().getClass()
                                        .getCanonicalName().toLowerCase(), DocumentIndexer.PARENT_ID_FIELD,
                                        e.getEntityId(), childClazz.getCanonicalName().toLowerCase());
                                // System.out.println(query);
                                Map<String, String> results = childClient.getIndexManager().search(query);
                                Set<String> rsSet = new HashSet<String>(results.values());
                                // childs = (List<Object>)
                                // childClient.find(childClazz,
                                // rsSet.toArray(new String[] {}));

                                if (childClazz.equals(e.getEntity().getClass()))
                                {
                                    childs = (List<Object>) childClient.findAll(childClazz,
                                            rsSet.toArray(new String[] {}));
                                }
                                else
                                {
                                    childs = (List<Object>) persistenceDelegeator.find(childClazz, g,
                                            rsSet.toArray(new String[] {}));
                                }

                            }
                        }
                        // secondary indexes.
                        // create sql query for hibernate client.
                        // f = g.getProperty();
                        collectionHolder.put(relationalValue, childs);
                        if (childs != null)
                        {
                            for (Object child : childs)
                            {
                                onBiDirection(e, client, g, m, child, childMetadata, childClient);
                            }
                        }
                    }
                    // handle bi direction here.

                    onReflect(e.getEntity(), f, (List) collectionHolder.get(relationalValue));

                }

            }
            // this is a single place holder for bi direction.
        }
        return e.getEntity();
    }

    /**
     * On reflect.
     * 
     * @param entity
     *            the entity
     * @param f
     *            the f
     * @param childs
     *            the childs
     * @return the sets the
     * @throws PropertyAccessException
     *             the property access exception
     */
    private Set<?> onReflect(Object entity, Field f, List<?> childs) throws PropertyAccessException
    {
        Set chids = new HashSet();
        if (childs != null)
        {
            chids = new HashSet(childs);
            // TODO: need to store object in sesion.
            // getSession().store(id, entity)
            PropertyAccessorHelper.set(entity, f,
                    PropertyAccessorHelper.isCollection(f.getType()) ? getFieldInstance(childs, f) : childs.get(0));
        }
        return chids;
    }

    /**
     * Gets the field instance.
     * 
     * @param chids
     *            the chids
     * @param f
     *            the f
     * @return the field instance
     */
    private Object getFieldInstance(List chids, Field f)
    {

        if (Set.class.isAssignableFrom(f.getType()))
        {
            Set col = new HashSet(chids);
            return col;
        }
        return chids;
    }

    /**
     * Returns lucene based query.
     * 
     * @param clazzFieldName
     *            lucene field name for class
     * @param clazzName
     *            class name
     * @param idFieldName
     *            lucene id field name
     * @param idFieldValue
     *            lucene id field value
     * @param entityClazz
     *            the entity clazz
     * @return query lucene query.
     */
    protected static String getQuery(String clazzFieldName, String clazzName, String idFieldName, String idFieldValue,
            String entityClazz)
    {
        StringBuffer sb = new StringBuffer("+");
        sb.append(clazzFieldName);
        sb.append(":");
        sb.append(clazzName);
        sb.append(" AND ");
        sb.append("+");
        sb.append(idFieldName);
        sb.append(":");
        sb.append(idFieldValue);
        if (entityClazz != null)
        {
            sb.append(" AND ");
            sb.append("+");
            sb.append(DocumentIndexer.ENTITY_CLASS_FIELD);
            sb.append(":");
            sb.append(entityClazz);
        }
        return sb.toString();
    }

    /**
     * On bi direction.
     * 
     * @param e
     *            the e
     * @param client
     *            the client
     * @param objectGraph
     *            the object graph
     * @param origMetadata
     *            the orig metadata
     * @param child
     *            the child
     * @param childMetadata
     *            the child metadata
     * @param childClient
     *            the child client
     * @throws Exception
     *             the exception
     */
    private void onBiDirection(EnhanceEntity e, Client client, EntitySaveGraph objectGraph,
            EntityMetadata origMetadata, Object child, EntityMetadata childMetadata, Client childClient)
            throws Exception
    {
        // Process bidirectional processing only if it's a bidirectional graph
        // and child exists (there is a possibility that this
        // child make already have been deleted
        if (!objectGraph.isUniDirectional() && child != null)
        {
            // Add original fetched entity.
            List obj = new ArrayList();

            Relation relation = childMetadata.getRelation(objectGraph.getBidirectionalProperty().getName());

            // join column name is pk of entity and
            // If relation is One to Many or MANY TO MANY for associated
            // entity. Require to fetch all associated entity

            if (relation.getType().equals(ForeignKey.ONE_TO_MANY) || relation.getType().equals(ForeignKey.MANY_TO_MANY))
            {
                String query = null;
                try
                {
                    String id = PropertyAccessorHelper.getId(child, childMetadata);
                    List<Object> results = null;

                    if (MetadataUtils.useSecondryIndex(client.getPersistenceUnit()))
                    {
                        if (origMetadata.isRelationViaJoinTable())
                        {
                            Relation joinTableRelation = origMetadata.getRelation(objectGraph.getProperty().getName());
                            JoinTableMetadata jtMetadata = joinTableRelation.getJoinTableMetadata();
                            String joinTableName = jtMetadata.getJoinTableName();

                            Set<String> joinColumns = jtMetadata.getJoinColumns();
                            Set<String> inverseJoinColumns = jtMetadata.getInverseJoinColumns();

                            String joinColumnName = (String) joinColumns.toArray()[0];
                            String inverseJoinColumnName = (String) inverseJoinColumns.toArray()[0];

                            List<Object> parentEntities = client.findParentEntityFromJoinTable(origMetadata,
                                    joinTableName, joinColumnName, inverseJoinColumnName, id);

                            if (results == null)
                            {
                                results = new ArrayList<Object>();
                            }

                            if (parentEntities != null)
                            {
                                results.addAll(parentEntities);
                            }

                        }
                        else
                        {
                            results = client.find(objectGraph.getfKeyName(), id, origMetadata);
                        }

                    }
                    else
                    {
                        Map<String, String> keys = null;
                        if (relation.getType().equals(ForeignKey.ONE_TO_MANY))
                        {
                            query = getQuery(DocumentIndexer.PARENT_ID_CLASS, child.getClass().getCanonicalName()
                                    .toLowerCase(), DocumentIndexer.PARENT_ID_FIELD, id, e.getEntity().getClass()
                                    .getCanonicalName().toLowerCase());
                            // System.out.println(query);
                            keys = client.getIndexManager().search(query);
                        }
                        else
                        {
                            query = getQuery(DocumentIndexer.ENTITY_CLASS_FIELD, child.getClass().getCanonicalName()
                                    .toLowerCase(), DocumentIndexer.ENTITY_ID_FIELD, id, null);
                            keys = client.getIndexManager().fetchRelation(query);

                        }
                        Set<String> uqSet = new HashSet<String>(keys.values());
                        results = new ArrayList<Object>();
                        for (String rowKey : uqSet)
                        {
                            Object result = client.find(e.getEntity().getClass(), origMetadata, rowKey, null);
                            if (result != null)
                            {
                                results.add(result);
                            }

                        }
                    }

                    if (results != null)
                    {
                        obj.addAll(results);
                    }
                }
                catch (PropertyAccessException ex)
                {
                    log.error("error on handling bi direction:" + ex.getMessage());
                    throw new QueryHandlerException(ex.getMessage());
                }

                // In case of other parent object found for given
                // bidirectional.
                for (Object o : obj)
                {
                    if (o != null)
                    {
                        Field f = objectGraph.getProperty();
                        if (PropertyAccessorHelper.isCollection(f.getType()))
                        {
                            List l = new ArrayList();
                            l.add(child);
                            Object oo = getFieldInstance(l, f);
                            PropertyAccessorHelper.set(o, f, oo);
                        }
                        else
                        {
                            PropertyAccessorHelper.set(o, f, child);
                        }
                    }

                }
            }
            else
            {
                obj.add(e.getEntity());
            }
            try
            {
                PropertyAccessorHelper
                        .set(child,
                                objectGraph.getBidirectionalProperty(),
                                PropertyAccessorHelper.isCollection(objectGraph.getBidirectionalProperty().getType()) ? getFieldInstance(
                                        obj, objectGraph.getBidirectionalProperty()) : e.getEntity());
            }
            catch (PropertyAccessException ex)
            {
                log.error("error on handling bi direction:" + ex.getMessage());
                throw new QueryHandlerException(ex.getMessage());
            }
        }
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
        try
        {
            List resultList = client.findAll(m.getEntityClazz(), rSet.toArray(new String[] {}));
            return transform(m, ls, resultList);
        }
        catch (Exception e)
        {
            log.error("Error while executing handleAssociation for cassandra:" + e.getMessage());
            throw new QueryHandlerException(e.getMessage());
        }

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
        Map<String, String> results = client.getIndexManager().search(luceneQueryFromJPAQuery);
        Set<String> rSet = new HashSet<String>(results.values());
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
    protected String getId(Object entity, EntityMetadata metadata)
    {
        try
        {
            return PropertyAccessorHelper.getId(entity, metadata);
        }
        catch (PropertyAccessException e)
        {
            throw new PersistenceException(e.getMessage());
        }

    }

    /**
     * Compute join table relations.
     * 
     * @param e
     *            the e
     * @param client
     *            the client
     * @param entityMetadata
     *            the entity metadata
     * @param objectGraph
     *            the object graph
     * @param delegator
     *            the delegator
     * @param relation
     *            the relation
     */
    private void computeJoinTableRelations(EnhanceEntity e, Client client, EntityMetadata entityMetadata,
            EntitySaveGraph objectGraph, PersistenceDelegator delegator, Relation relation)
    {

        Object entity = e.getEntity();

        objectGraph.setParentId(getId(entity, entityMetadata));
        JoinTableMetadata jtMetadata = relation.getJoinTableMetadata();
        String joinTableName = jtMetadata.getJoinTableName();

        Set<String> joinColumns = jtMetadata.getJoinColumns();
        Set<String> inverseJoinColumns = jtMetadata.getInverseJoinColumns();

        String joinColumnName = (String) joinColumns.toArray()[0];
        String inverseJoinColumnName = (String) inverseJoinColumns.toArray()[0];

        EntityMetadata relMetadata = delegator.getMetadata(objectGraph.getChildClass());

        Client pClient = delegator.getClient(entityMetadata);
        List<?> foreignKeys = pClient.getForeignKeysFromJoinTable(joinTableName, joinColumnName, inverseJoinColumnName,
                relMetadata, objectGraph);

        List childrenEntities = new ArrayList();
        for (Object foreignKey : foreignKeys)
        {
            try
            {
                EntityMetadata childMetadata = delegator.getMetadata(relation.getTargetEntity());
                Client childClient = delegator.getClient(childMetadata);
                // Object child = childClient.find(relation.getTargetEntity(),
                // childMetadata, (String) foreignKey, null);
                Object child = delegator.find(relation.getTargetEntity(), foreignKey);

                onBiDirection(e, client, objectGraph, entityMetadata, child, childMetadata, childClient);

                childrenEntities.add(child);

            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }
        }

        Field childField = objectGraph.getProperty();

        try
        {
            PropertyAccessorHelper.set(
                    entity,
                    childField,
                    PropertyAccessorHelper.isCollection(childField.getType()) ? getFieldInstance(childrenEntities,
                            childField) : childrenEntities.get(0));
        }
        catch (PropertyAccessException ex)
        {
            ex.printStackTrace();
        }

    }

}
