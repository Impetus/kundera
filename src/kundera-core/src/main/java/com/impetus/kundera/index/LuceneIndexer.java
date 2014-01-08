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

import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Fields;
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
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.cache.ElementCollectionCacheManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

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
    private LuceneIndexer(Analyzer analyzer, String lucDirPath)
    {
        super(analyzer);
        try
        {
            luceneDirPath = lucDirPath;
            File file = new File(luceneDirPath);
            if (file.exists())
            {
                Directory sourceDir = FSDirectory.open(getIndexDirectory());

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
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_34, analyzer);
            LogDocMergePolicy logDocMergePolicy = new LogDocMergePolicy();
            logDocMergePolicy.setMergeFactor(1000);
            indexWriterConfig.setMergePolicy(logDocMergePolicy);
            w = new IndexWriter(index, indexWriterConfig);
            /* reader = */
            // w.setMergePolicy(new LogDocMergePolicy());
            // w.setMergeFactor(1);
            // w.setMergeFactor(1000);
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
    public static synchronized LuceneIndexer getInstance(Analyzer analyzer, String lucDirPath)
    {
        // super(analyzer);
        if (indexer == null && lucDirPath != null)
        {
            indexer = new LuceneIndexer(analyzer, lucDirPath);

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
        flushInternal();

        if (reader == null)
        {
            try
            {
                if (!isInitialized)
                {
                    Directory sourceDir = FSDirectory.open(getIndexDirectory());
                    copy(sourceDir, index);
                    isInitialized = true;
                }
                reader = IndexReader.open(index/* , true */);
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
    public final void index(EntityMetadata metadata, Object object)
    {
        indexDocument(metadata, object, null, null);
        onCommit();
    }

    @Override
    public final void unindex(EntityMetadata metadata, Object id) throws LuceneIndexingException
    {
        if (log.isDebugEnabled())
            log.debug("Unindexing @Entity[{}] for key:{}", metadata.getEntityClazz().getName() , id);
        try
        {
            QueryParser qp = new QueryParser(Version.LUCENE_34, DEFAULT_SEARCHABLE_FIELD, new StandardAnalyzer(
                    Version.LUCENE_34));

            qp.setLowercaseExpandedTerms(false);
            qp.setAllowLeadingWildcard(true);

            String luceneQuery = "+"
                    + ENTITY_CLASS_FIELD
                    + ":"
                    + QueryParser.escape(metadata.getEntityClazz().getCanonicalName().toLowerCase())
                    + " AND +"
                    + getCannonicalPropertyName(QueryParser.escape(metadata.getEntityClazz().getSimpleName()),
                            QueryParser.escape(((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName()))
                    + ":" + QueryParser.escape(id.toString());

            /* String indexName, Query query, boolean autoCommit */
            // w.deleteDocuments(new Term(KUNDERA_ID_FIELD,
            // getKunderaId(metadata, id)));

            // qp.set

            Query q = qp.parse(luceneQuery);

            w.deleteDocuments(q);
            w.commit();
            w.close();
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_34, analyzer);
            LogDocMergePolicy logDocMergePolicy = new LogDocMergePolicy();
            logDocMergePolicy.setMergeFactor(1000);
            indexWriterConfig.setMergePolicy(logDocMergePolicy);
            w = new IndexWriter(index, indexWriterConfig);
            /* reader = */
            // w.setMergePolicy(new LogDocMergePolicy());
            // w.setMergeFactor(1);
            // w.setMergeFactor(1000);
            w.getConfig().setRAMBufferSizeMB(32);
            // flushInternal();
        }
        catch (Exception e)
        {
            log.error("Error while instantiating LuceneIndexer, Caused by :.", e);
            throw new LuceneIndexingException(e);
        }
    }

    @Override
    public final void update(EntityMetadata metadata, Object entity, Object id, String parentId)

    {
        if (log.isDebugEnabled()) 
        {
            log.debug("Updating @Entity[{}] for key:{}" ,metadata.getEntityClazz().getName(), id);
        }

        updateDocument(metadata, entity, parentId, entity.getClass(), true);

    }

   

    @SuppressWarnings("deprecation")
    @Override
    public final Map<String, Object> search(String luceneQuery, int start, int count, boolean fetchRelation)
    {

        reader = getIndexReader();

        if (Constants.INVALID == count)
        {
            count = 100;
        }

        if (log.isDebugEnabled()) 
        {
            log.debug("Searching index with query[{}], start:{} , count:" + count,luceneQuery,start);
        }

        // Set<String> entityIds = new HashSet<String>();
        Map<String, Object> indexCol = new HashMap<String, Object>();

        if (reader == null)
        {
            return indexCol;
            // throw new
            // LuceneIndexingException("Index reader is not initialized!");
        }

        IndexSearcher searcher = new IndexSearcher(reader);
        QueryParser qp = new QueryParser(Version.LUCENE_34, DEFAULT_SEARCHABLE_FIELD, new StandardAnalyzer(
                Version.LUCENE_34));

        try
        {
            qp.setLowercaseExpandedTerms(false);
            qp.setAllowLeadingWildcard(true);
            // qp.set
            Query q = qp.parse(luceneQuery);
            TopDocs docs = searcher.search(q, count);

            int nullCount = 0;
            // Assuming Supercol will be null in case if alias only.
            // This is a quick fix

            for (ScoreDoc sc : docs.scoreDocs)
            {

                Document doc = searcher.doc(sc.doc);
                String entityId = doc.get(fetchRelation ? PARENT_ID_FIELD : ENTITY_ID_FIELD);
                String superCol = doc.get(SUPERCOLUMN_INDEX);

                if (superCol == null)
                {
                    superCol = "SuperCol" + nullCount++;
                }
                // In case of super column and association.
                indexCol.put(superCol + "|" + entityId, entityId);
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
            // w.setR
            w.addDocument(document);
            // w.optimize();
            // w.commit();
            // w.close();
        }
        catch (Exception e)
        {
            log.error("Error while indexing document {} into Lucene, Caused by:{} ",document, e);
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
    public void updateDocument(String id, Document document)
    {
        if (log.isDebugEnabled()) 
        {
            log.debug("Updateing indexed document: {} for in file system using Lucene", document);
        }

        IndexWriter w = getIndexWriter();
        try
        {
            Term term = new Term(ENTITY_ID_FIELD, id);
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
                copy(index, FSDirectory.open(getIndexDirectory()));
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
                copy(index, FSDirectory.open(getIndexDirectory()));
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
    public void index(EntityMetadata metadata, Object object, String parentId, Class<?> clazz)
    {

        indexDocument(metadata, object, parentId, clazz);
        onCommit();
    }

    @Override
    public boolean entityExistsInIndex(Class<?> entityClass)
    {
        String luceneQuery = "+" + ENTITY_CLASS_FIELD + ":" + entityClass.getCanonicalName().toLowerCase();
        Map<String, Object> results;
        try
        {
            results = search(luceneQuery, 0, 10, false);
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
    public boolean documentExistsInIndex(EntityMetadata metadata, Object id)
    {
        String luceneQuery = "+"
                + ENTITY_CLASS_FIELD
                + ":"
                + QueryParser.escape(metadata.getEntityClazz().getCanonicalName().toLowerCase())
                + " AND +"
                + getCannonicalPropertyName(QueryParser.escape(metadata.getEntityClazz().getSimpleName()),
                        QueryParser.escape(((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName())) + ":"
                + QueryParser.escape(id.toString());

        Map<String, Object> results;
        try
        {
            results = search(luceneQuery, 0, 10, false);

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
    private Document indexDocument(EntityMetadata metadata, Object object, String parentId, Class<?> clazz)
    {

        if (log.isDebugEnabled()) 
        {
            log.debug("Indexing @Entity[{}],{} " ,metadata.getEntityClazz().getName(), object);
        }

        // In case defined entity is Super column family.
        // we need to create seperate lucene document for indexing.
        Document currentDoc = updateOrIndexDocument(metadata, object, parentId, clazz, false);

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
    private Document updateOrIndexDocument(EntityMetadata metadata, Object entity, String parentId, Class<?> clazz,
            boolean isUpdate)
    {
        if (!metadata.isIndexable())
        {
            return null;
        }

        Document currentDoc = null;
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

            MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                    metadata.getPersistenceUnit());

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
                    ElementCollectionCacheManager ecCacheHandler = ElementCollectionCacheManager.getInstance();
                    // Check whether it's first time insert or updation
                    if (ecCacheHandler.isCacheEmpty())
                    { // First time
                      // insert
                        int count = 0;
                        for (Object obj : (Collection<?>) embeddedObject)
                        {
                            String elementCollectionObjectName = attributeName
                                    + Constants.EMBEDDED_COLUMN_NAME_DELIMITER + count;

                            currentDoc = prepareDocumentForSuperColumn(metadata, entity, elementCollectionObjectName,
                                    parentId, clazz);
                            createSuperColumnDocument(metadata, entity, currentDoc, obj, embeddableAttribute);
                            if (isUpdate)
                            {
                                updateDocument(parentId, currentDoc);

                            }
                            else
                            {
                               indexDocument(metadata, currentDoc);
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
                            String elementCollectionObjectName = ecCacheHandler.getElementCollectionObjectName(rowKey,
                                    obj);
                            if (elementCollectionObjectName == null)
                            { // Fresh
                              // row
                                elementCollectionObjectName = attributeName + Constants.EMBEDDED_COLUMN_NAME_DELIMITER
                                        + (++lastEmbeddedObjectCount);
                            }

                            currentDoc = prepareDocumentForSuperColumn(metadata, entity, elementCollectionObjectName,
                                    parentId, clazz);
                            createSuperColumnDocument(metadata, entity, currentDoc, obj, embeddableAttribute);
                            if (isUpdate)
                            {
                                updateDocument(parentId, currentDoc);
                            }
                            else
                            {
                                indexDocument(metadata, currentDoc);
                            }
                        }
                    }

                }
                else
                {
                    currentDoc = prepareDocumentForSuperColumn(metadata, entity, attributeName, parentId, clazz);
                    createSuperColumnDocument(metadata, entity, currentDoc,
                            metaModel.isEmbeddable(embeddedObject.getClass()) ? embeddedObject : entity,
                            embeddableAttribute);
                    if (isUpdate)
                    {
                       updateDocument(parentId, currentDoc);
                    }
                    else
                    {
                        indexDocument(metadata, currentDoc);
                    }
                }
            }
        }
        else
        {
            currentDoc = new Document();

            // Add entity class, PK info into document

            addEntityClassToDocument(metadata, entity, currentDoc);

            // Add all entity fields(columns) into document
            addEntityFieldsToDocument(metadata, entity, currentDoc);

            addParentKeyToDocument(parentId, currentDoc, clazz);
            // Store document into index
            if (isUpdate)
            {
                updateDocument(rowKey.toString(), currentDoc);
            }
            else
            {
                indexDocument(metadata, currentDoc);
            }

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
    }

    @Override
    public void index(Class entityClazz, Map<String, Object> values, Object parentId, final Class parentClazz)
    {
        throw new UnsupportedOperationException("Method not supported");
    }

    @Override
    public void unIndex(Class entityClazz, Object entity)
    {
        throw new UnsupportedOperationException("Method not supported");
    }

    @Override
    public Map<String, Object> search(Class<?> clazz, String luceneQuery, int start, int end)
    {
        throw new UnsupportedOperationException("Method not supported");
    }

    private void copy(Directory src, Directory to) throws IOException
    {
        for (String file : src.listAll())
        {
            src.copy(to, file, file, IOContext.DEFAULT);
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
    private void updateDocument(EntityMetadata metadata, Object entity, String parentId,
            Class<? extends Object> class1, boolean b)
    {
        updateOrIndexDocument(metadata, entity, parentId, entity.getClass(), true);
        onCommit();

    }

}
