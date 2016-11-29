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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.RAMDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.cache.ElementCollectionCacheManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * Provides indexing functionality using lucene library.
 * 
 * @author amresh.singh
 */
public class LuceneIndexer extends DocumentIndexer
{

    /** log for this class. */
    private static Logger log = LoggerFactory.getLogger(LuceneIndexer.class);

    /** The w. */
    private static IndexWriter w;

    /** The reader. */
    private static IndexReader reader;

    /** The index. */
    private static Directory index;

    /** The is initialized. */
    private static boolean isInitialized;

    /** The indexer. */
    private static LuceneIndexer indexer;

    /** The ready for commit. */
    private static boolean readyForCommit;

    /** The lucene dir path. */
    private static String luceneDirPath;

    /**
     * Instantiates a new lucene indexer.
     * 
     * @param analyzer
     *            the analyzer
     * @param lucDirPath
     *            the luc dir path
     */
    private LuceneIndexer(String lucDirPath)
    {
        try
        {
            luceneDirPath = lucDirPath;
            File file = new File(luceneDirPath);
            if (file.exists())
            {
                FSDirectory sourceDir = FSDirectory.open(getIndexDirectory().toPath());

                // TODO initialize context.
                index = new RAMDirectory(sourceDir, IOContext.DEFAULT);
            }
            else
            {
                index = new RAMDirectory();
            }
            /*
             * FSDirectory.open(getIndexDirectory( ))
             */
            // isInitialized
            /* writer */
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
            LogDocMergePolicy logDocMergePolicy = new LogDocMergePolicy();
            logDocMergePolicy.setMergeFactor(1000);
            indexWriterConfig.setMergePolicy(logDocMergePolicy);
            w = new IndexWriter(index, indexWriterConfig);
            w.getConfig().setRAMBufferSizeMB(32);
        }
        catch (Exception e)
        {
            log.error("Error while instantiating LuceneIndexer, Caused by :.", e);
            throw new LuceneIndexingException(e);
        }
    }

    /**
     * Gets the single instance of LuceneIndexer.
     * 
     * @param analyzer
     *            the analyzer
     * @param lucDirPath
     *            the luc dir path
     * @return single instance of LuceneIndexer
     */
    public static synchronized LuceneIndexer getInstance(String lucDirPath)
    {
        // super(analyzer);
        if (indexer == null && lucDirPath != null)
        {
            indexer = new LuceneIndexer(lucDirPath);

        }
        return indexer;
    }

    /**
     * Added for HBase support.
     * 
     * @return default index writer
     */
    private IndexWriter getIndexWriter()
    {
        return w;
    }

    /**
     * Returns default index reader.
     * 
     * @return index reader.
     */
    private IndexReader getIndexReader()
    {
        // removed flushInternal() call while reading
        if (reader == null)
        {
            try
            {
                if (!isInitialized)
                {
                    Directory sourceDir = FSDirectory.open(getIndexDirectory().toPath());
                    copy(sourceDir, index);
                    isInitialized = true;
                }
                reader = DirectoryReader.open(index);
            }
            catch (IndexNotFoundException infex)
            {
                log.warn("No index found in given directory, caused by:", infex.getMessage());
            }
            catch (Exception e)
            {
                log.error("Error while instantiating LuceneIndexer, Caused by :.", e);
                throw new LuceneIndexingException(e);
            }
        }
        return reader;
    }

    /**
     * Creates a Lucene index directory if it does not exist.
     * 
     * @return the index directory
     */
    private File getIndexDirectory()
    {
        File file = new File(luceneDirPath);

        if (!file.isDirectory())
        {
            file.mkdir();
        }
        return file;
    }

    @Override
    public final void index(EntityMetadata metadata, final MetamodelImpl metaModel, Object object)
    {
        indexDocument(metadata, metaModel, object, null, null);
        onCommit();
    }

    @Override
    public final void unindex(EntityMetadata metadata, Object id, KunderaMetadata kunderaMetadata, Class<?> parentClazz)
            throws LuceneIndexingException
    {
        if (log.isDebugEnabled())
            log.debug("Unindexing @Entity[{}] for key:{}", metadata.getEntityClazz().getName(), id);
        String luceneQuery = null;
        boolean isEmbeddedId = false;

        MetamodelImpl metaModel = null;
        if (kunderaMetadata != null && metadata != null)
        {
            metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    metadata.getPersistenceUnit());
            isEmbeddedId = metaModel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType());
        }

        try
        {
            QueryParser qp = new QueryParser(DEFAULT_SEARCHABLE_FIELD, new StandardAnalyzer());

            qp.setLowercaseExpandedTerms(false);
            qp.setAllowLeadingWildcard(true);
            luceneQuery = getLuceneQuery(metadata, id, isEmbeddedId, metaModel, parentClazz);
            Query q = qp.parse(luceneQuery);

            w.deleteDocuments(q);
            w.commit();
            w.close();
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
            LogDocMergePolicy logDocMergePolicy = new LogDocMergePolicy();
            logDocMergePolicy.setMergeFactor(1000);
            indexWriterConfig.setMergePolicy(logDocMergePolicy);
            w = new IndexWriter(index, indexWriterConfig);

            w.getConfig().setRAMBufferSizeMB(32);
            // flushInternal();
        }
        catch (Exception e)
        {
            log.error("Error while instantiating LuceneIndexer, Caused by :.", e);
            throw new LuceneIndexingException(e);
        }
    }

    /**
     * @param metadata
     * @param id
     * @param isEmbeddedId
     * @param metaModel
     * @return
     */
    private String getLuceneQuery(EntityMetadata metadata, Object id, boolean isEmbeddedId, MetamodelImpl metaModel,
            Class<?> parentClazz)
    {
        StringBuilder luceneQuery = new StringBuilder("+").append(IndexingConstants.ENTITY_CLASS_FIELD).append(":")
                .append(QueryParser.escape(metadata.getEntityClazz().getCanonicalName().toLowerCase()))
                .append(" AND +");
        if (isEmbeddedId)
        {
            id = KunderaCoreUtils.prepareCompositeKey(metadata.getIdAttribute(), metaModel, id);
            luceneQuery.append(IndexingConstants.ENTITY_ID_FIELD).append(":").append(QueryParser.escape(id.toString()));
        }
        else
        {
            luceneQuery
                    .append(getCannonicalPropertyName(QueryParser.escape(metadata.getEntityClazz().getSimpleName()),
                            QueryParser.escape(((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName())))
                    .append(":").append(QueryParser.escape(id.toString()));

        }
        if (parentClazz != null)
        {
            luceneQuery.append(" AND +").append(IndexingConstants.PARENT_ID_CLASS).append(":")
                    .append(parentClazz.getCanonicalName().toLowerCase());
        }
        return luceneQuery.toString();
    }

    @Override
    public final void update(EntityMetadata metadata, final MetamodelImpl metaModel, Object entity, Object id,
            String parentId)
    {
        if (log.isDebugEnabled())
        {
            log.debug("Updating @Entity[{}] for key:{}", metadata.getEntityClazz().getName(), id);
        }

        updateDocument(metadata, metaModel, entity, parentId, entity.getClass(), true);
    }

    /**
     * search the data from lucene for embeddedid
     * 
     * @param docs
     * @param indexCol
     * @param searcher
     * @param metadata
     * @param metaModel
     */
    public void prepareEmbeddedId(TopDocs docs, Map<String, Object> indexCol, IndexSearcher searcher,
            EntityMetadata metadata, MetamodelImpl metaModel)
    {
        try
        {
            for (ScoreDoc sc : docs.scoreDocs)
            {
                Document doc = searcher.doc(sc.doc);
                Map<String, Object> embeddedIdFields = new HashMap<String, Object>();
                EmbeddableType embeddableId = metaModel.embeddable(metadata.getIdAttribute().getBindableJavaType());
                Set<Attribute> embeddedAttributes = embeddableId.getAttributes();

                prepareEmbeddedIdFields(embeddedAttributes, metaModel, embeddedIdFields, doc, metadata);

                String entityId = doc.get(IndexingConstants.ENTITY_ID_FIELD);
                indexCol.put(entityId, embeddedIdFields);
            }
        }
        catch (Exception e)
        {
            log.error("Error while parsing Lucene Query {} ", e);
            throw new LuceneIndexingException(e);
        }
    }

    private void prepareEmbeddedIdFields(Set<Attribute> embeddedAttributes, MetamodelImpl metaModel,
            Map<String, Object> embeddedIdFields, Document doc, EntityMetadata metadata)
    {

        for (Attribute attribute : embeddedAttributes)
        {
            if (!ReflectUtils.isTransientOrStatic((Field) attribute.getJavaMember()))
            {
                if (metaModel.isEmbeddable(attribute.getJavaType()))
                {
                    EmbeddableType embeddable = metaModel.embeddable(attribute.getJavaType());
                    prepareEmbeddedIdFields(embeddable.getAttributes(), metaModel, embeddedIdFields, doc, metadata);
                }
                else
                {
                    String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                    embeddedIdFields.put(columnName,
                            doc.get(metadata.getEntityClazz().getSimpleName() + "." + columnName));
                }
            }
        }
    }

    @Override
    public final Map<String, Object> search(String luceneQuery, int start, int count, boolean fetchRelation,
            KunderaMetadata kunderaMetadata, EntityMetadata metadata)
    {
        boolean isEmbeddedId = false;

        MetamodelImpl metaModel = null;
        if (kunderaMetadata != null && metadata != null)
        {
            metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    metadata.getPersistenceUnit());
            isEmbeddedId = metaModel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType());
        }
        reader = getIndexReader();

        if (Constants.INVALID == count)
        {
            count = 100;
        }

        if (log.isDebugEnabled())
        {
            log.debug("Searching index with query[{}], start:{} , count:" + count, luceneQuery, start);
        }

        // Set<String> entityIds = new HashSet<String>();
        Map<String, Object> indexCol = new HashMap<String, Object>();

        if (reader == null)
        {

            return indexCol;
            // throw new
            // LuceneIndexingException("Index reader is not initialized!");
        }
        QueryParser qp = null;
        IndexSearcher searcher = new IndexSearcher(reader);

        qp = new QueryParser(DEFAULT_SEARCHABLE_FIELD, new StandardAnalyzer());

        try
        {
            // to make like query case insensitive
            // qp.setLowercaseExpandedTerms(true);
            qp.setAllowLeadingWildcard(true);
            // qp.set
            Query q = qp.parse(luceneQuery);

            TopDocs docs = searcher.search(q, count);

            int nullCount = 0;

            // Assuming Supercol will be null in case if alias only.
            // This is a quick fix
            if (isEmbeddedId)
            {
                prepareEmbeddedId(docs, indexCol, searcher, metadata, metaModel);
            }
            else
            {
                for (ScoreDoc sc : docs.scoreDocs)
                {

                    Document doc = searcher.doc(sc.doc);
                    String entityId = doc.get(fetchRelation ? IndexingConstants.PARENT_ID_FIELD
                            : IndexingConstants.ENTITY_ID_FIELD);
                    String superCol = doc.get(SUPERCOLUMN_INDEX);

                    if (superCol == null)
                    {
                        superCol = "SuperCol" + nullCount++;
                    }
                    // In case of super column and association.
                    indexCol.put(superCol + "|" + entityId, entityId);
                }

            }
        }
        catch (Exception e)
        {
            log.error("Error while parsing Lucene Query {} ", luceneQuery, e);
            throw new LuceneIndexingException(e);
        }

        reader = null;
        return indexCol;
    }

    /**
     * Indexes document in file system using lucene.
     * 
     * @param metadata
     *            the metadata
     * @param document
     *            the document
     */
    public void indexDocument(EntityMetadata metadata, Document document)
    {
        if (log.isDebugEnabled())
        {
            log.debug("Indexing document: {} for in file system using Lucene", document);
        }

        IndexWriter w = getIndexWriter();
        try
        {
            w.addDocument(document);
        }
        catch (Exception e)
        {
            log.error("Error while indexing document {} into Lucene, Caused by:{} ", document, e);
            throw new LuceneIndexingException("Error while indexing document " + document + " into Lucene.", e);
        }
    }

    /**
     * Indexes document in file system using lucene.
     * 
     * @param metadata
     *            the metadata
     * @param document
     *            the document
     */
    public void updateDocument(String id, Document document, String EmbeddedEntityFieldName)
    {
        if (log.isDebugEnabled())
        {
            log.debug("Updateing indexed document: {} for in file system using Lucene", document);
        }

        IndexWriter w = getIndexWriter();
        try
        {
            Term term = null;
            if (EmbeddedEntityFieldName == null)
            {
                term = new Term(IndexingConstants.ENTITY_ID_FIELD, id);
            }
            else
            {
                term = new Term(EmbeddedEntityFieldName, id);
            }
            w.updateDocument(term, document);

        }
        catch (LuceneIndexingException lie)
        {
            log.error("Error while updating LuceneIndexer, Caused by :.", lie);
            throw new LuceneIndexingException(lie);
        }
        catch (IOException ioe)
        {
            log.error("Error while reading Lucene indexes, Caused by :.", ioe);

        }
    }

    /**
     * Flush internal.
     */
    private void flushInternal()
    {
        try
        {
            if (w != null && readyForCommit)
            {
                // w.optimize();
                w.commit();
                copy(index, FSDirectory.open(getIndexDirectory().toPath()));
                readyForCommit = false;
                reader = null;
                isInitialized = false;
            }
        }

        catch (Exception e)
        {
            log.error("Error while Flushing Lucene Indexes, Caused by: ", e);
            throw new LuceneIndexingException("Error while Flushing Lucene Indexes", e);
        }
    }

    /**
     * Close of transaction.
     */
    public void close()
    {
        try
        {
            if (w != null && readyForCommit)
            {
                w.commit();
                copy(index, FSDirectory.open(getIndexDirectory().toPath()));
            }
        }

        catch (Exception e)
        {
            log.error("Error while closing lucene indexes, Caused by: ", e);
            throw new LuceneIndexingException("Error while closing lucene indexes.", e);
        }
    }

    @Override
    public void flush()
    {
        /*
         * if (w != null) {
         * 
         * try { w.commit(); // w.close(); // index.copy(index,
         * FSDirectory.open(getIndexDirectory()), false); } catch
         * (CorruptIndexException e) { } catch (IOException e) { } }
         */
    }

    @Override
    public void index(EntityMetadata metadata, final MetamodelImpl metaModel, Object object, String parentId,
            Class<?> clazz)
    {

        indexDocument(metadata, metaModel, object, parentId, clazz);
        onCommit();
    }

    @Override
    public boolean entityExistsInIndex(Class<?> entityClass, KunderaMetadata kunderaMetadata, EntityMetadata metadata)
    {
        String luceneQuery = "+" + IndexingConstants.ENTITY_CLASS_FIELD + ":"
                + entityClass.getCanonicalName().toLowerCase();
        Map<String, Object> results;
        try
        {
            results = search(luceneQuery, 0, 10, false, kunderaMetadata, metadata);
        }
        catch (LuceneIndexingException e)
        {
            return false;
        }
        if (results == null || results.isEmpty())
        {
            return false;
        }
        else
        {
            return true;
        }
    }

    @Override
    public boolean documentExistsInIndex(EntityMetadata metadata, Object id, KunderaMetadata kunderaMetadata,
            boolean isEmbeddedId, Class<?> parentClazz)
    {
        String luceneQuery = null;

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                metadata.getPersistenceUnit());

        luceneQuery = getLuceneQuery(metadata, id, isEmbeddedId, metaModel, parentClazz);
        Map<String, Object> results;
        try
        {
            results = search(luceneQuery, 0, 10, false, kunderaMetadata, metadata);
        }
        catch (LuceneIndexingException e)
        {
            results = null;
        }

        return !(results == null || results.isEmpty());
    }

    /**
     * Index document.
     * 
     * @param metadata
     *            the metadata
     * @param object
     *            the object
     * @param parentId
     *            the parent id
     * @param clazz
     *            the clazz
     * @return the document
     */
    private Document indexDocument(EntityMetadata metadata, final MetamodelImpl metaModel, Object object,
            String parentId, Class<?> clazz)
    {

        if (log.isDebugEnabled())
        {
            log.debug("Indexing @Entity[{}],{} ", metadata.getEntityClazz().getName(), object);
        }

        // In case defined entity is Super column family.
        // we need to create seperate lucene document for indexing.
        Document currentDoc = updateOrCreateIndex(metadata, metaModel, object, parentId, clazz, false);

        return currentDoc;

    }

    /**
     * Update/Index document.
     * 
     * @param metadata
     *            the metadata
     * @param entity
     *            the object
     * @param parentId
     *            the parent id
     * @param clazz
     *            the clazz
     * @return the document
     */
    private Document updateOrCreateIndex(EntityMetadata metadata, final MetamodelImpl metaModel, Object entity,
            String parentId, Class<?> clazz, boolean isUpdate)
    {
        boolean isEmbeddedId = metaModel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType());
        if (!metadata.isIndexable())
        {
            return null;
        }

        Document document = null;
        Object embeddedObject = null;
        Object rowKey = null;

        try
        {
            rowKey = PropertyAccessorHelper.getId(entity, metadata);
        }
        catch (PropertyAccessException e1)
        {
            throw new LuceneIndexingException("Can't access Primary key property from " + metadata.getEntityClazz(), e1);
        }

        if (metadata.getType().equals(EntityMetadata.Type.SUPER_COLUMN_FAMILY))
        {
            Map<String, EmbeddableType> embeddables = metaModel.getEmbeddables(metadata.getEntityClazz());

            Iterator<String> iter = embeddables.keySet().iterator();

            while (iter.hasNext())
            {

                String attributeName = iter.next();
                EmbeddableType embeddableAttribute = embeddables.get(attributeName);
                EntityType entityType = metaModel.entity(metadata.getEntityClazz());

                embeddedObject = PropertyAccessorHelper.getObject(entity, (Field) entityType
                        .getAttribute(attributeName).getJavaMember());

                if (embeddedObject == null)
                {
                    continue;
                }
                if (embeddedObject instanceof Collection<?>)
                {
                    document = updateOrCreateIndexCollectionTypeEmbeddedObject(metadata, metaModel, entity, parentId,
                            clazz, isUpdate, document, embeddedObject, rowKey, attributeName, embeddableAttribute);
                }
                else
                {
                    document = prepareDocumentForSuperColumn(metadata, entity, attributeName, parentId, clazz);
                    createSuperColumnDocument(metadata, entity, document,
                            metaModel.isEmbeddable(embeddedObject.getClass()) ? embeddedObject : entity,
                            embeddableAttribute, metaModel);
                    if (isUpdate)
                    {
                        updateDocument(parentId, document, null);
                    }
                    else
                    {
                        indexDocument(metadata, document);
                    }
                }
            }
        }
        else
        {
            document = updateOrCreateIndexNonSuperColumnFamily(metadata, metaModel, entity, parentId, clazz, isUpdate,
                    isEmbeddedId, rowKey);
        }
        return document;

    }

    /**
     * update or Create Index for non super columnfamily
     * 
     * @param metadata
     * @param metaModel
     * @param entity
     * @param parentId
     * @param clazz
     * @param isUpdate
     * @param isEmbeddedId
     * @param rowKey
     * @return
     */
    private Document updateOrCreateIndexNonSuperColumnFamily(EntityMetadata metadata, final MetamodelImpl metaModel,
            Object entity, String parentId, Class<?> clazz, boolean isUpdate, boolean isEmbeddedId, Object rowKey)
    {

        Document document = new Document();

        // Add entity class, PK info into document

        addEntityClassToDocument(metadata, entity, document, metaModel);

        // Add all entity fields(columns) into document
        addEntityFieldsToDocument(metadata, entity, document, metaModel);

        addAssociatedEntitiesToDocument(metadata, entity, document, metaModel);
        addParentKeyToDocument(parentId, document, clazz);
        if (isUpdate)
        {
            if (isEmbeddedId)
            {
                // updating delimited composite key
                String compositeId = KunderaCoreUtils.prepareCompositeKey(metadata.getIdAttribute(), metaModel, rowKey);
                updateDocument(compositeId, document, null);
                // updating sub parts of composite key
                EmbeddableType embeddableId = metaModel.embeddable(metadata.getIdAttribute().getBindableJavaType());
                Set<Attribute> embeddedAttributes = embeddableId.getAttributes();

                updateOrCreateIndexEmbeddedIdFields(embeddedAttributes, metaModel, document, metadata, rowKey);
            }
            else
            {
                updateDocument(rowKey.toString(), document, null);
            }
        }
        else
        {
            indexDocument(metadata, document);
        }
        return document;
    }

    private void updateOrCreateIndexEmbeddedIdFields(Set<Attribute> embeddedAttributes, MetamodelImpl metaModel,
            Document document, EntityMetadata metadata, Object rowKey)
    {
        try
        {
            for (Attribute attribute : embeddedAttributes)
            {
                if (!ReflectUtils.isTransientOrStatic((Field) attribute.getJavaMember()))
                {
                    if (metaModel.isEmbeddable(attribute.getJavaType()))
                    {
                        EmbeddableType embeddable = metaModel.embeddable(attribute.getJavaType());
                        updateOrCreateIndexEmbeddedIdFields(embeddable.getAttributes(), metaModel, document, metadata,
                                ((Field) attribute.getJavaMember()).get(rowKey));
                    }
                    else
                    {
                        String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                        Object embeddedColumn = PropertyAccessorHelper.getObject(rowKey,
                                (Field) attribute.getJavaMember());
                        updateDocument(embeddedColumn.toString(), document, columnName);
                    }
                }
            }
        }
        catch (IllegalAccessException e)
        {
            log.error(e.getMessage());
        }
    }

    /**
     * update or create indexes when embedded object is of collection type
     * 
     * @param metadata
     * @param metaModel
     * @param entity
     * @param parentId
     * @param clazz
     * @param isUpdate
     * @param document
     * @param embeddedObject
     * @param rowKey
     * @param attributeName
     * @param embeddableAttribute
     * @return
     */
    private Document updateOrCreateIndexCollectionTypeEmbeddedObject(EntityMetadata metadata,
            final MetamodelImpl metaModel, Object entity, String parentId, Class<?> clazz, boolean isUpdate,
            Document document, Object embeddedObject, Object rowKey, String attributeName,
            EmbeddableType embeddableAttribute)
    {
        ElementCollectionCacheManager ecCacheHandler = ElementCollectionCacheManager.getInstance();
        // Check whether it's first time insert or updation
        if (ecCacheHandler.isCacheEmpty())
        { // First time
          // insert
            int count = 0;
            for (Object obj : (Collection<?>) embeddedObject)
            {
                String elementCollectionObjectName = attributeName + Constants.EMBEDDED_COLUMN_NAME_DELIMITER + count;

                document = prepareDocumentForSuperColumn(metadata, entity, elementCollectionObjectName, parentId, clazz);
                createSuperColumnDocument(metadata, entity, document, obj, embeddableAttribute, metaModel);
                if (isUpdate)
                {
                    updateDocument(parentId, document, null);
                }
                else
                {
                    indexDocument(metadata, document);
                }
                count++;
            }
        }
        else
        {
            // Updation, Check whether this object is already in
            // cache, which means we already have an embedded
            // column
            // Otherwise we need to generate a fresh embedded
            // column name
            int lastEmbeddedObjectCount = ecCacheHandler.getLastElementCollectionObjectCount(rowKey);
            for (Object obj : (Collection<?>) embeddedObject)
            {
                document = indexCollectionObject(metadata, entity, parentId, clazz, isUpdate, rowKey, attributeName,
                        embeddableAttribute, ecCacheHandler, lastEmbeddedObjectCount, obj, metaModel);
            }
        }
        return document;
    }

    private Document indexCollectionObject(EntityMetadata metadata, Object entity, String parentId, Class<?> clazz,
            boolean isUpdate, Object rowKey, String attributeName, EmbeddableType embeddableAttribute,
            ElementCollectionCacheManager ecCacheHandler, int lastEmbeddedObjectCount, Object obj,
            MetamodelImpl metamodel)
    {
        Document currentDoc;
        String elementCollectionObjectName = ecCacheHandler.getElementCollectionObjectName(rowKey, obj);
        if (elementCollectionObjectName == null)
        { // Fresh
          // row
            elementCollectionObjectName = attributeName + Constants.EMBEDDED_COLUMN_NAME_DELIMITER
                    + (++lastEmbeddedObjectCount);
        }

        currentDoc = prepareDocumentForSuperColumn(metadata, entity, elementCollectionObjectName, parentId, clazz);
        createSuperColumnDocument(metadata, entity, currentDoc, obj, embeddableAttribute, metamodel);
        if (isUpdate)
        {
            updateDocument(parentId, currentDoc, null);
        }
        else
        {
            indexDocument(metadata, currentDoc);
        }
        return currentDoc;
    }

    /**
     * On commit.
     */
    private void onCommit()
    {
        // TODO: Sadly this required to keep lucene happy, in case of indexing
        // and searching with same entityManager.
        // Other alternative would be to issue flush on each search
        // try
        // {
        // w.commit();
        isInitialized = true;
        readyForCommit = true;
        // }
        // catch (CorruptIndexException e)
        // {
        // throw new IndexingException(e.getMessage());
        // }
        // catch (IOException e)
        // {
        // throw new IndexingException(e.getMessage());
        // }
        flushInternal();
    }

    @Override
    public void index(Class entityClazz, EntityMetadata entityMetadata, Map<String, Object> values, Object parentId,
            final Class parentClazz)
    {
        throw new UnsupportedOperationException("Method not supported");
    }

    @Override
    public void unIndex(Class entityClazz, Object entity, EntityMetadata metadata, MetamodelImpl metamodel)
    {
        throw new UnsupportedOperationException("Method not supported");
    }

    @Override
    public Map<String, Object> search(Class<?> clazz, EntityMetadata m, String luceneQuery, int start, int end)
    {
        throw new UnsupportedOperationException("Method not supported");
    }

    private void copy(Directory src, Directory to) throws IOException
    {
        for (String file : src.listAll())
        {
            src.copyFrom(src, file, to.toString(), IOContext.DEFAULT);
        }
    }

    /**
     * Updates document.
     * 
     * @param metadata
     *            the metadata
     * @param entity
     *            the object
     * @param parentId
     *            the parent id
     * @param clazz
     *            the clazz
     * @return the document
     */
    private void updateDocument(EntityMetadata metadata, final MetamodelImpl metaModel, Object entity, String parentId,
            Class<? extends Object> class1, boolean b)
    {
        updateOrCreateIndex(metadata, metaModel, entity, parentId, entity.getClass(), true);
        onCommit();
    }

    @Override
    public Map<String, Object> search(KunderaMetadata kunderaMetadata, KunderaQuery kunderaQuery,
            PersistenceDelegator persistenceDelegator, EntityMetadata m, int firstResult, int maxResults)
    {
        return null;
    }

}