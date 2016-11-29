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

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import com.impetus.kundera.Constants;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PropertyIndex;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;

/**
 * Manager responsible to co-ordinate with an Indexer. It is bound with
 * EntityManager.
 * 
 * @author animesh.kumar
 */
public class IndexManager
{

    /** The indexer. */
    private final Indexer indexer;

    private final KunderaMetadata kunderaMetadata;

    /**
     * The Constructor.
     * 
     * @param indexer
     *            the indexer
     */
    @SuppressWarnings("deprecation")
    public IndexManager(Indexer indexer, final KunderaMetadata kunderaMetadata)
    {
        this.indexer = indexer;
        this.kunderaMetadata = kunderaMetadata;
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
    public final void remove(EntityMetadata metadata, Object entity, Object key)
    {
        if (indexer != null)
        {
            if (indexer.getClass().getName().equals(IndexingConstants.LUCENE_INDEXER))
            {
                ((com.impetus.kundera.index.lucene.Indexer) indexer).unindex(metadata, key, kunderaMetadata, null);
            }
            else
            {
                indexer.unIndex(metadata.getEntityClazz(), entity, metadata, (MetamodelImpl) kunderaMetadata
                        .getApplicationMetadata().getMetamodel(metadata.getPersistenceUnit()));
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
            if (indexer != null)
            {
                if (indexer.getClass().getName().equals(IndexingConstants.LUCENE_INDEXER))
                {
                    MetamodelImpl metamodel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                            metadata.getPersistenceUnit());
                    Object id = PropertyAccessorHelper.getId(entity, metadata);
                    boolean isEmbeddedId = metamodel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType());

                    boolean documentExistsInIndex = ((com.impetus.kundera.index.lucene.Indexer) indexer)
                            .documentExistsInIndex(metadata, id, kunderaMetadata, isEmbeddedId, clazz);

                    if (documentExistsInIndex)
                    {
                        ((com.impetus.kundera.index.lucene.Indexer) indexer).update(metadata, metamodel, entity, id,
                                parentId != null ? parentId.toString() : null);
                    }
                    else
                    {

                        boolean documentExists = ((com.impetus.kundera.index.lucene.Indexer) indexer)
                                .entityExistsInIndex(entity.getClass(), kunderaMetadata, metadata);
                        if (documentExists)
                        {
                            ((com.impetus.kundera.index.lucene.Indexer) indexer).unindex(metadata, id, kunderaMetadata,
                                    clazz);
                            ((com.impetus.kundera.index.lucene.Indexer) indexer).flush();
                        }
                        ((com.impetus.kundera.index.lucene.Indexer) indexer).index(metadata, metamodel, entity,
                                parentId != null ? parentId.toString() : null, clazz);
                    }
                }
                else
                {
                    MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                            metadata.getPersistenceUnit());

                    Map<String, PropertyIndex> indexProperties = metadata.getIndexProperties();
                    Map<String, Object> indexCollection = new HashMap<String, Object>();
                    Object id = PropertyAccessorHelper.getId(entity, metadata);
                    for (String columnName : indexProperties.keySet())
                    {
                        PropertyIndex index = indexProperties.get(columnName);
                        Field property = index.getProperty();
                        // String propertyName = index.getName();
                        Object obj = PropertyAccessorHelper.getObject(entity, property);
                        indexCollection.put(columnName, obj);
                    }

                    indexCollection.put(((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName(), id);

                    EntityMetadata parentMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz);
                    if (parentId != null)
                        indexCollection.put(((AbstractAttribute) parentMetadata.getIdAttribute()).getJPAColumnName(),
                                parentId);

                    onEmbeddable(entity, metadata.getEntityClazz(), metaModel, indexCollection);
                    indexer.index(metadata.getEntityClazz(), metadata, indexCollection, parentId, clazz);
                }
            }
        }
        catch (PropertyAccessException e)
        {
            throw new IndexingException("Can't access ID from entity class " + metadata.getEntityClazz(), e);
        }
    }

    /**
     * @param entity
     * @param clazz
     * @param metaModel
     * @param indexCollection
     */
    private void onEmbeddable(Object entity, Class<?> clazz, MetamodelImpl metaModel,
            Map<String, Object> indexCollection)
    {
        Map<String, EmbeddableType> embeddables = metaModel.getEmbeddables(clazz);
        EntityType entityType = metaModel.entity(clazz);

        for (String embeddedFieldName : embeddables.keySet())
        {
            EmbeddableType embeddedColumn = embeddables.get(embeddedFieldName);

            // Index embeddable only when specified by user
            Field embeddedField = (Field) entityType.getAttribute(embeddedFieldName).getJavaMember();
            if (!MetadataUtils.isEmbeddedAtributeIndexable(embeddedField))
            {
                continue;
            }

            Object embeddedObject = PropertyAccessorHelper.getObject(entity,
                    (Field) entityType.getAttribute(embeddedFieldName).getJavaMember());
            if (embeddedObject != null && !(embeddedObject instanceof Collection))
            {
                for (Object column : embeddedColumn.getAttributes())
                {
                    Attribute columnAttribute = (Attribute) column;
                    String columnName = columnAttribute.getName();
                    Class<?> columnClass = ((AbstractAttribute) columnAttribute).getBindableJavaType();
                    if (MetadataUtils.isColumnInEmbeddableIndexable(embeddedField, columnName))
                    {
                        indexCollection.put(
                                embeddedField.getName() + "." + columnName,
                                PropertyAccessorHelper.getObject(embeddedObject,
                                        (Field) columnAttribute.getJavaMember()));
                    }
                }
            }

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
        if (indexer != null)
        {
            MetamodelImpl metamodel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    metadata.getPersistenceUnit());
            ((com.impetus.kundera.index.lucene.Indexer) indexer).index(metadata, metamodel, entity);
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
        if (indexer != null)
        {
            MetamodelImpl metamodel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    metadata.getPersistenceUnit());
            ((com.impetus.kundera.index.lucene.Indexer) indexer).index(metadata, metamodel, entity, parentId, clazz);
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
    // TODO: All lucene specific code (methods that accept lucene query as
    // parameter) from this class should go away
    // and should be moved to LuceneIndexer instead
    public final Map<String, Object> search(Class<?> clazz, String query)
    {

        return search(clazz, query, Constants.INVALID, Constants.INVALID, false);
    }

    public final Map<String, Object> search(Class<?> parentClass, Class<?> childClass, Object entityId)
    {
        if (indexer == null)
            return null;

        // Ideally it should be interface driven and should be handled by
        // fallback-impl.

        if (indexer != null && indexer.getClass().getName().equals(IndexingConstants.LUCENE_INDEXER))
        {

            // Search into Lucene index using lucene query, where entity class
            // is child class, parent class is
            // entity's class and parent Id is entity ID! that's it!
            String query = LuceneQueryUtils.getQuery(IndexingConstants.PARENT_ID_CLASS, parentClass.getCanonicalName()
                    .toLowerCase(), IndexingConstants.PARENT_ID_FIELD, entityId, childClass.getCanonicalName()
                    .toLowerCase());
            return ((com.impetus.kundera.index.lucene.Indexer) indexer).search(query, Constants.INVALID,
                    Constants.INVALID, false, kunderaMetadata,
                    KunderaMetadataManager.getEntityMetadata(kunderaMetadata, parentClass));
        }
        else
        {
            String query = LuceneQueryUtils.getQuery(IndexingConstants.PARENT_ID_CLASS, parentClass.getCanonicalName()
                    .toLowerCase(), IndexingConstants.PARENT_ID_FIELD, entityId, childClass.getCanonicalName()
                    .toLowerCase());
            // If an alternate indexer implementation class is provided by user,
            // search into that
            return indexer.search(query, parentClass,
                    KunderaMetadataManager.getEntityMetadata(kunderaMetadata, parentClass), childClass,
                    KunderaMetadataManager.getEntityMetadata(kunderaMetadata, childClass), entityId, Constants.INVALID,
                    Constants.INVALID);
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
    public final Map<String, Object> fetchRelation(Class<?> clazz, String query)
    {
        // TODO: need to return list.
        return search(clazz, query, Constants.INVALID, Constants.INVALID, true);
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
    public final Map<String, Object> search(Class<?> clazz, String query, int count)
    {
        return search(clazz, query, Constants.INVALID, count, false);
    }

    /**
     * Search.
     *
     * @param clazz
     *            the clazz
     * @param query
     *            the query
     * @param start
     *            the start
     * @param count
     *            the count
     * @return the list
     */
    public final Map<String, Object> search(Class<?> clazz, String query, int start, int count)
    {
        if (indexer != null)
        {
            if (indexer != null && indexer.getClass().getName().equals(IndexingConstants.LUCENE_INDEXER))
            {
                return indexer != null ? ((com.impetus.kundera.index.lucene.Indexer) indexer)
                        .search(query, start, count, false, kunderaMetadata,
                                KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz)) : null;
            }
            else
            {
                return indexer.search(clazz, KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz), query,
                        start, count);
            }
        }
        return new HashMap<String, Object>();
    }

    public final Map<String, Object> search(KunderaMetadata kunderaMetadata, KunderaQuery kunderaQuery,
            PersistenceDelegator persistenceDelegator, EntityMetadata m,int firstResult, int maxResults)
    {
        return indexer.search(kunderaMetadata, kunderaQuery, persistenceDelegator, m, firstResult, maxResults);
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
    public final Map<String, Object> search(Class<?> clazz, String query, int start, int count, boolean fetchRelation)
    {
        if (indexer != null)
        {
            if (indexer.getClass().getName().equals(IndexingConstants.LUCENE_INDEXER))
            {
                return indexer != null ? ((com.impetus.kundera.index.lucene.Indexer) indexer).search(query, start,
                        count, fetchRelation, kunderaMetadata,
                        KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz)) : null;
            }
            else
            {
                return indexer.search(clazz, KunderaMetadataManager.getEntityMetadata(kunderaMetadata, clazz), query,
                        start, count);
            }
        }
        return new HashMap<String, Object>();
    }

    /**
     * Flushes out the indexes, keeping RAM directory open.
     */
    public void flush() throws IndexingException
    {
        if (indexer != null)
        {
            // ((Indexer) indexer).close();
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