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
package com.impetus.kundera.index;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.metamodel.Metamodel;

import com.impetus.kundera.Constants;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PropertyIndex;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Manager responsible to co-ordinate with an Indexer. It is bound with
 * EntityManager.
 * 
 * @author animesh.kumar
 */
public class IndexManager
{

    /** The indexer. */
    private Indexer indexer;

    /**
     * The Constructor.
     * 
     * @param indexer
     *            the indexer
     */
    @SuppressWarnings("deprecation")
    public IndexManager(Indexer indexer)
    {
        this.indexer = indexer;
    }    
    

    /**
     * @return the indexer
     */
    public Indexer getIndexer()
    {
        return indexer;
    }


    /**
     * Removes an object from Index.
     * 
     * @param metadata
     *            the metadata
     * @param entity
     *            the entity
     * @param key
     *            the key
     */
    public final void remove(EntityMetadata metadata, Object entity, String key)
    {
        if (indexer != null)
        {

            if (!MetadataUtils.useSecondryIndex(metadata.getPersistenceUnit())
                    && indexer.getClass().isAssignableFrom(LuceneIndexer.class))
            {
                ((com.impetus.kundera.index.lucene.Indexer) indexer).unindex(metadata, key);
            }
            else
            {
                indexer.unIndex(metadata.getEntityClazz(), key);
            }
        }
    }

    /**
     * Updates the index for an object.
     * 
     * @param metadata
     *            the metadata
     * @param entity
     *            the entity
     */
    public final void update(EntityMetadata metadata, Object entity, Object parentId, Class<?> clazz)
    {

        try
        {
            if (!MetadataUtils.useSecondryIndex(metadata.getPersistenceUnit()) && indexer != null
                    && indexer.getClass().isAssignableFrom(LuceneIndexer.class))
            {
                Object id = PropertyAccessorHelper.getId(entity, metadata);

                boolean documentExists = ((com.impetus.kundera.index.lucene.Indexer) indexer)
                        .entityExistsInIndex(entity.getClass());
                if (documentExists)
                {
                    ((com.impetus.kundera.index.lucene.Indexer) indexer).unindex(metadata, id);
                    ((com.impetus.kundera.index.lucene.Indexer) indexer).flush();
                }
                ((com.impetus.kundera.index.lucene.Indexer) indexer)
                        .index(metadata, entity, parentId != null ? parentId.toString() : null, clazz);
            }
            else
            {
                MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata()
                        .getMetamodel(metadata.getPersistenceUnit());

                Map<String, PropertyIndex> indexProperties = metadata.getIndexProperties();
                Map<String, Object> indexCollection = new HashMap<String, Object>();
                Object id = PropertyAccessorHelper.getId(entity, metadata);
                for (String columnName : indexProperties.keySet())
                {
                    PropertyIndex index = indexProperties.get(columnName);
                    java.lang.reflect.Field property = index.getProperty();
                    // String propertyName = index.getName();
                    Object obj = PropertyAccessorHelper.getObject(entity, property);
                    indexCollection.put(columnName, obj);
                    
                }
                
                //indexCollection.put(DocumentIndexer.ENTITY_CLASS_FIELD, metadata.getEntityClazz().getCanonicalName().toLowerCase());
                indexCollection.put(((AbstractAttribute)metadata.getIdAttribute()).getJPAColumnName(), id);            
                
                EntityMetadata parentMetadata = KunderaMetadataManager.getEntityMetadata(clazz);
                if(parentId != null)
                    indexCollection.put(((AbstractAttribute)parentMetadata.getIdAttribute()).getJPAColumnName(), parentId);
                
                indexer.index(metadata.getEntityClazz(), indexCollection);
            }
        }
        catch (PropertyAccessException e)
        {
            throw new IndexingException("Can't access ID from entity class " + metadata.getEntityClazz(), e);
        }
    }

    /**
     * Indexes an object.
     * 
     * @param metadata
     *            the metadata
     * @param entity
     *            the entity
     */
    public final void write(EntityMetadata metadata, Object entity)
    {
        if (!MetadataUtils.useSecondryIndex(metadata.getPersistenceUnit()))
        {
            ((com.impetus.kundera.index.lucene.Indexer) indexer).index(metadata, entity);
        }
    }

    /**
     * Indexes an object.
     * 
     * @param metadata
     *            the metadata
     * @param entity
     *            the entity
     * @param parentId
     *            parent Id.
     * @param clazz
     *            class name
     */
    public final void write(EntityMetadata metadata, Object entity, String parentId, Class<?> clazz)
    {
        if (!MetadataUtils.useSecondryIndex(metadata.getPersistenceUnit()))
        {
            
            
            ((com.impetus.kundera.index.lucene.Indexer) indexer).index(metadata, entity, parentId, clazz);
        }
    }

    /**
     * Searches on the index. Note: Query must be in Indexer's understandable
     * format
     * 
     * @param query
     *            the query
     * @return the list
     */
    @Deprecated
    //TODO: All lucene specific code (methods that accept lucene query as parameter) from this class should go away
    //and should be moved to LuceneIndexer instead
    public final Map<String, Object> search(String query)
    {

        return search(query, Constants.INVALID, Constants.INVALID, false);
    }

    public final Map<String, Object> search(Class<?> parentClass, Class<?> childClass, Object entityId)
    {        
        if(indexer == null) return null;        
        
        if (indexer != null && indexer.getClass().isAssignableFrom(LuceneIndexer.class))
        {
            
            // Search into Lucene index using lucene query, where entity class is child class, parent class is
            // entity's class and parent Id is entity ID! that's it!
            String query = LuceneQueryUtils.getQuery(DocumentIndexer.PARENT_ID_CLASS, parentClass.getCanonicalName()
                    .toLowerCase(), DocumentIndexer.PARENT_ID_FIELD, entityId, childClass.getCanonicalName().toLowerCase());
            return ((com.impetus.kundera.index.lucene.Indexer) indexer).search(query, Constants.INVALID, Constants.INVALID,
                    false);
        }
        else
        {
            //If an alternate indexer implementation class is provided by user, search into that
            return indexer.search(parentClass, childClass, entityId, Constants.INVALID, Constants.INVALID);
        }   
        
    }

    /**
     * Searches on the index. Note: Query must be in Indexer's understandable
     * format
     * 
     * @param query
     *            the query
     * @return the list
     */
    public final Map<String, Object> fetchRelation(String query)
    {
        // TODO: need to return list.
        return search(query, Constants.INVALID, Constants.INVALID, true);
    }

    /**
     * Search.
     * 
     * @param query
     *            the query
     * @param count
     *            the count
     * @return the list
     */
    public final Map<String, Object> search(String query, int count)
    {
        return search(query, Constants.INVALID, count, false);
    }

    /**
     * Search.
     * 
     * @param query
     *            the query
     * @param start
     *            the start
     * @param count
     *            the count
     * @return the list
     */
    public final Map<String, Object> search(String query, int start, int count)
    {
        if (indexer != null && indexer.getClass().isAssignableFrom(LuceneIndexer.class))
        {
            return indexer != null ? ((com.impetus.kundera.index.lucene.Indexer) indexer).search(query, start, count,
                    false) : null;
        }
        else
        {
            return indexer.search(query, start, count);
        }
    }

    /**
     * Search.
     * 
     * @param query
     *            the query
     * @param start
     *            the start
     * @param count
     *            the count
     * @param fetchRelation
     *            the fetch relation
     * @return the list
     */
    public final Map<String, Object> search(String query, int start, int count, boolean fetchRelation)
    {
        if (indexer != null && indexer.getClass().isAssignableFrom(LuceneIndexer.class))
        {
            return indexer != null ? ((com.impetus.kundera.index.lucene.Indexer) indexer).search(query, start, count,
                    fetchRelation) : null;
        }
        else
        {
            return indexer.search(query, start, count);
        }
    }

    /**
     * Flushes out the indexes, keeping RAM directory open.
     */
    public void flush() throws IndexingException
    {
        if (indexer != null)
        {
            ((com.impetus.kundera.index.lucene.Indexer) indexer).flush();
        }
    }

    /**
     * Closes the transaction along with RAM directory.
     */
    public void close() throws IndexingException
    {
        if (indexer != null)
        {
            indexer.close();
        }
    }
}
