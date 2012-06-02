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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.graph.Node;
import com.impetus.kundera.graph.ObjectGraphUtils;
import com.impetus.kundera.index.DocumentIndexer;
import com.impetus.kundera.index.LuceneQueryUtils;
import com.impetus.kundera.lifecycle.states.ManagedState;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.JoinTableMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.context.MainCache;
import com.impetus.kundera.persistence.context.PersistenceCacheManager;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.ObjectUtils;

/**
 * This class is responsible for building association for given entities.
 * 
 * @author vivek.mishra
 */
final class AssociationBuilder
{
    
    private static Log log = LogFactory.getLog(AssociationBuilder.class);

    
    
    /**
     * Populates entities related via join table for <code>entity</code>
     * @param entity
     * @param entityMetadata
     * @param delegator
     * @param relation
     */
    void populateRelationFromJoinTable(Object entity, EntityMetadata entityMetadata,
            PersistenceDelegator delegator, Relation relation)
    {        

        JoinTableMetadata jtMetadata = relation.getJoinTableMetadata();
        String joinTableName = jtMetadata.getJoinTableName();

        Set<String> joinColumns = jtMetadata.getJoinColumns();
        Set<String> inverseJoinColumns = jtMetadata.getInverseJoinColumns();

        String joinColumnName = (String) joinColumns.toArray()[0];
        String inverseJoinColumnName = (String) inverseJoinColumns.toArray()[0];

        //EntityMetadata relMetadata = delegator.getMetadata(relation.getTargetEntity());

        Client pClient = delegator.getClient(entityMetadata);
        String entityId = PropertyAccessorHelper.getId(entity, entityMetadata);
        List<?> foreignKeys = pClient.getColumnsById(joinTableName, joinColumnName, inverseJoinColumnName,
                entityId);

        List childrenEntities = new ArrayList();
        for (Object foreignKey : foreignKeys)
        {
            EntityMetadata childMetadata = delegator.getMetadata(relation.getTargetEntity());
            

            Object child = delegator.find(relation.getTargetEntity(), foreignKey);

            Object obj = child instanceof EnhanceEntity && child != null ? ((EnhanceEntity) child).getEntity() : child;            
            
            //If child has any bidirectional relationship, process them here
            Field biDirectionalField = getBiDirectionalField(entity.getClass(), relation.getTargetEntity());
            boolean isBidirectionalRelation = (biDirectionalField != null);
            
            if(isBidirectionalRelation && obj != null) {      
                
                String columnValue = PropertyAccessorHelper.getId(obj, childMetadata);
                Object[] pKeys = pClient.findIdsByColumn(joinTableName, joinColumnName,
                        inverseJoinColumnName, columnValue, entityMetadata.getEntityClazz());             
                List parents = delegator.find(entity.getClass(), pKeys);                
                PropertyAccessorHelper.set(obj, biDirectionalField, ObjectUtils.getFieldInstance(parents, biDirectionalField));              
                
            }    
            
            childrenEntities.add(obj);
        }

        Field childField = relation.getProperty();

        try
        {
            PropertyAccessorHelper.set(
                    entity,
                    childField,
                    PropertyAccessorHelper.isCollection(childField.getType()) ? ObjectUtils.getFieldInstance(childrenEntities,
                            childField) : childrenEntities.get(0));
            PersistenceCacheManager.addEntityToPersistenceCache(entity, delegator, entityId);
        }
        catch (PropertyAccessException ex)
        {
            throw new EntityReaderException(ex);
        }

    }        
    
    /**
     * @param entity
     * @param pd
     * @param relation
     * @param relationValue
     */
    void populateRelationFromValue(Object entity, PersistenceDelegator pd, Relation relation,
            Object relationValue, EntityMetadata childMetadata)
    {
        Class<?> childClass = relation.getTargetEntity();        
        
        Object child = pd.find(childClass, relationValue.toString());
        child = child != null && child instanceof EnhanceEntity ? ((EnhanceEntity) child)
                .getEntity() : child;
        
        PropertyAccessorHelper.set(entity, relation.getProperty(), child);
        
        //If child has any bidirectional relationship, process them here
        Field biDirectionalField = getBiDirectionalField(entity.getClass(), relation.getTargetEntity());
        boolean isBidirectionalRelation = (biDirectionalField != null);
        
        if(isBidirectionalRelation) {
            Relation reverseRelation = childMetadata.getRelation(biDirectionalField.getName());
            String childId = PropertyAccessorHelper.getId(child, childMetadata);
            EntityMetadata reverseEntityMetadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
            populateRelationViaQuery(child, pd, childId, reverseRelation, relation.getJoinColumnName(), reverseEntityMetadata);
        }  
        
        //Save this entity to persistence cache
        /*String entityId = PropertyAccessorHelper.getId(entity, KunderaMetadataManager.getEntityMetadata(entity.getClass()));
        MainCache mainCache = (MainCache) pd.getPersistenceCache().getMainCache();        
        String nodeId = ObjectGraphUtils.getNodeId(entityId, entity.getClass());
        Node node = new Node(nodeId, entity.getClass(), new ManagedState(), pd.getPersistenceCache());
        node.setData(entity);        
        node.setPersistenceDelegator(pd);
        mainCache.addNodeToCache(node);  */
    }
    
    /**
     * @param entity
     * @param pd
     * @param entityId
     * @param relation
     * @param relationName
     */
    void populateRelationViaQuery(Object entity, PersistenceDelegator pd, String entityId, Relation relation,
            String relationName, EntityMetadata childMetadata)
    {
        Class<?> childClass = relation.getTargetEntity();                            
        Client childClient = pd.getClient(childMetadata);                    
        
        List children = null;
        
        //Since ID is stored at other side of the relationship, we have to query that table
                           
        if (MetadataUtils.useSecondryIndex(childClient.getPersistenceUnit()))        {
            //Pass this entity id as a value to be searched for 
            children = pd.find(childClass, entityId, relationName);
        }
        else
        {
            // Lucene query, where entity class is child class, parent class is entity's class
            // and parent Id is entity ID! that's it!
            String query = LuceneQueryUtils.getQuery(DocumentIndexer.PARENT_ID_CLASS, entity
                    .getClass().getCanonicalName().toLowerCase(),
                    DocumentIndexer.PARENT_ID_FIELD, entityId, childClass
                            .getCanonicalName().toLowerCase());
            
            Map<String, String> results = childClient.getIndexManager().search(query);
            Set<String> rsSet = new HashSet<String>(results.values());

            if (childClass.equals(entity.getClass()))
            {
                children = (List<Object>) childClient.findAll(childClass,
                        rsSet.toArray(new String[] {}));
            }
            else
            {
                children = (List<Object>) childClient.findAll(childClass,
                        rsSet.toArray(new String[] {}));
            }
        } 
        
        onReflect(entity, relation.getProperty(), children);
        
        //If child has any bidirectional relationship, process them here
        Field biDirectionalField = getBiDirectionalField(entity.getClass(), relation.getTargetEntity());
        boolean isBidirectionalRelation = (biDirectionalField != null);
        
        if(isBidirectionalRelation && children != null) {
            Relation reverseRelation = childMetadata.getRelation(biDirectionalField.getName());
            
            for(Object child : children) {
                //String childId = PropertyAccessorHelper.getId(child, childMetadata);
                //EntityMetadata reverseEntityMetadata = KunderaMetadataManager.getEntityMetadata(entity.getClass());
                
                //populateRelationFromValue(child, pd, reverseRelation, entityId, childMetadata);
                PropertyAccessorHelper.set(child, reverseRelation.getProperty(), entity);
            }
            
        }
        
        //Save this entity to persistence cache
        /*MainCache mainCache = (MainCache) pd.getPersistenceCache().getMainCache();        
        String nodeId = ObjectGraphUtils.getNodeId(entityId, entity.getClass());
        Node node = new Node(nodeId, entity.getClass(), new ManagedState(), pd.getPersistenceCache());
        node.setData(entity);        
        node.setPersistenceDelegator(pd);
        mainCache.addNodeToCache(node);    */        
        
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
            if (clazzz.equals(originalClazz))
            {
                biDirectionalField = field;
                break;
            }
        }

        return biDirectionalField;
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
                    PropertyAccessorHelper.isCollection(f.getType()) ? ObjectUtils.getFieldInstance(childs, f) : childs.get(0));
        }
        return chids;
    }

}
