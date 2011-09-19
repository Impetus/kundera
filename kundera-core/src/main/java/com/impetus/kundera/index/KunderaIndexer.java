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
package com.impetus.kundera.index;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import lucandra.IndexReader;

import org.apache.cassandra.db.RowMutation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

import com.impetus.kundera.Client;
import com.impetus.kundera.Constants;
import com.impetus.kundera.loader.DBType;
import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.PropertyIndex;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

// TODO: Auto-generated Javadoc
/**
 * The Class KunderaIndexer.
 * 
 * @author animesh.kumar
 */
public class KunderaIndexer implements Indexer
{

    /** log for this class. */
    private static final Log LOG = LogFactory.getLog(KunderaIndexer.class);

    /** The INDEX_NAME. */
    private static final String INDEX_NAME = "kundera-alpha";// is

    // persistent-unit-name

    /** The Constant UUID. */
    private static final long UUID = 6077004083174677888L;

    /** The Constant DELIMETER. */
    private static final String DELIMETER = "~";

    /** The Constant ENTITY_ID_FIELD. */
    public static final String ENTITY_ID_FIELD = UUID + ".entity.id";

    /** The Constant KUNDERA_ID_FIELD. */
    public static final String KUNDERA_ID_FIELD = UUID + ".kundera.id";

    /** The Constant ENTITY_INDEXNAME_FIELD. */
    public static final String ENTITY_INDEXNAME_FIELD = UUID + ".entity.indexname";

    /** The Constant ENTITY_CLASS_FIELD. */
    public static final String ENTITY_CLASS_FIELD = /* UUID + */"entity.class";

    /** The Constant DEFAULT_SEARCHABLE_FIELD. */
    private static final String DEFAULT_SEARCHABLE_FIELD = UUID + ".default_property";

    /** The Constant SUPERCOLUMN_INDEX. */
    private static final String SUPERCOLUMN_INDEX = UUID + ".entity.super.indexname";

    /** The doc number. */
    private static int docNumber = 1;

    /** The client. */
    private Client client;

    /** The analyzer. */
    private Analyzer analyzer;

    /**
     * Instantiates a new lucandra indexer.
     * 
     * @param client
     *            the client
     * @param analyzer
     *            the analyzer
     */
    public KunderaIndexer(Client client, Analyzer analyzer)
    {
        this.client = client;
        this.analyzer = analyzer;
    }

    @Override
    public final void unindex(EntityMetadata metadata, String id)
    {
        LOG.debug("Unindexing @Entity[" + metadata.getEntityClazz().getName() + "] for key:" + id);
        try
        {
            /* String indexName, Query query, boolean autoCommit */

            getIndexWriter().deleteDocuments(INDEX_NAME, new Term(KUNDERA_ID_FIELD, getKunderaId(metadata, id)), true);
        }
        catch (CorruptIndexException e)
        {
            throw new IndexingException(e.getMessage());
        }
        catch (IOException e)
        {
            throw new IndexingException(e.getMessage());
        }
    }

    @Override
    public final void index(EntityMetadata metadata, Object object)
    {

        if (!metadata.isIndexable())
        {
            return;
        }

        LOG.debug("Indexing @Entity[" + metadata.getEntityClazz().getName() + "] " + object);

        performIndexing(metadata, object);
    }

    /**
     * Perform indexing.
     * 
     * @param metadata
     *            the metadata
     * @param object
     *            the object
     */
    private void performIndexing(EntityMetadata metadata, Object object)
    {
        Document currentDoc = null;
        Object embeddedObject = null;
        // In case defined entity is Super column family.
        // we need to create seperate lucene document for indexing.
        if (metadata.getType().equals(EntityMetadata.Type.SUPER_COLUMN_FAMILY))
        {
            Map<String, EmbeddedColumn> superColMap = metadata.getEmbeddedColumnsMap();

            for (String superColumnName : superColMap.keySet())
            {
                EmbeddedColumn superColumn = superColMap.get(superColumnName);
                try
                {

                    embeddedObject = PropertyAccessorHelper.getObject(object, superColumn.getField());
                    // if embeddedObject is not set.
                    if (embeddedObject == null)
                    {
                        return;
                    }
                    if (embeddedObject instanceof Collection<?>)
                    {
                        for (Object obj : (Collection<?>) embeddedObject)
                        {
                            currentDoc = prepareDocument(metadata, object, superColumnName);
                            indexSuperColumn(metadata, object, currentDoc, obj, superColumn);
                        }
                        return;
                    }
                    else
                    {
                        currentDoc = prepareDocument(metadata, object, superColumnName);
                    }
                }
                catch (PropertyAccessException e)
                {
                    LOG.error("Error while accesing embedded Object:" + superColumnName);
                }
                indexSuperColumn(metadata, object, currentDoc,
                        metadata.isEmbeddable(embeddedObject.getClass()) ? embeddedObject : object, superColumn);
            }
        }
        else
        {
            currentDoc = new Document();
            prepareIndexDocument(metadata, object, currentDoc);
            addIndexProperties(metadata, object, currentDoc);
            indexDocument(metadata, currentDoc);
        }

    }

    /**
     * Prepare document.
     * 
     * @param metadata
     *            the metadata
     * @param object
     *            the object
     * @param superColumnName
     *            the super column name
     * @return the document
     */
    private Document prepareDocument(EntityMetadata metadata, Object object, String superColumnName)
    {
        Document currentDoc;
        currentDoc = new Document();
        prepareIndexDocument(metadata, object, currentDoc);
        indexSuperColumnName(superColumnName, currentDoc);
        return currentDoc;
    }

    /**
     * Index super column.
     * 
     * @param metadata
     *            the metadata
     * @param object
     *            the object
     * @param currentDoc
     *            the current doc
     * @param embeddedObject
     *            the embedded object
     * @param superColumn
     *            the super column
     */
    private void indexSuperColumn(EntityMetadata metadata, Object object, Document currentDoc, Object embeddedObject,
            EmbeddedColumn superColumn)
    {
        for (Column col : superColumn.getColumns())
        {
            java.lang.reflect.Field field = col.getField();
            String colName = col.getName();
            String indexName = metadata.getIndexName();
            indexField(embeddedObject, currentDoc, field, colName, indexName);
        }
        // add document.
        addIndexProperties(metadata, object, currentDoc);
        indexDocument(metadata, currentDoc);
    }

    /**
     * Index super column name.
     * 
     * @param superColumnName
     *            the super column name
     * @param currentDoc
     *            the current doc
     */
    private void indexSuperColumnName(String superColumnName, Document currentDoc)
    {
        Field luceneField = new Field(SUPERCOLUMN_INDEX, superColumnName, Field.Store.YES,
                Field.Index.ANALYZED_NO_NORMS);
        currentDoc.add(luceneField);

    }

    /**
     * Indexes document. For Cassandra it uses Lucandra library, for others it
     * simply indexes into file system using Lucene
     * 
     * @param metadata
     *            the metadata
     * @param document
     *            the document
     */
    private void indexDocument(EntityMetadata metadata, Document document)
    {
        try
        {
            LOG.debug("Indexing document: " + document + " for " + metadata.getDBType());
            if (metadata.getDBType().equals(DBType.CASSANDRA))
            {
                LOG.debug("Indexing document using Lucandra: " + document);
                indexDocumentUsingLucandra(document);
            }
            else
            {
                LOG.debug("Indexing document in file system using lucene: " + document);
                indexDocumentUsingLucene(document);
            }
        }
        catch (CorruptIndexException e)
        {
            throw new IndexingException(e.getMessage());
        }
        catch (IOException e)
        {
            throw new IndexingException(e.getMessage());
        }
    }

    /**
     * Adds the index properties.
     * 
     * @param metadata
     *            the metadata
     * @param object
     *            the object
     * @param document
     *            the document
     */
    private void addIndexProperties(EntityMetadata metadata, Object object, Document document)
    {
        String indexName = metadata.getIndexName();
        for (PropertyIndex index : metadata.getIndexProperties())
        {

            java.lang.reflect.Field property = index.getProperty();
            String propertyName = index.getName();
            indexField(object, document, property, propertyName, indexName);
        }
    }

    /**
     * Prepare index document.
     * 
     * @param metadata
     *            the metadata
     * @param object
     *            the object
     * @param document
     *            the document
     */
    private void prepareIndexDocument(EntityMetadata metadata, Object object, Document document)
    {
        try
        {

            Field luceneField;
            String id;
            id = PropertyAccessorHelper.getId(object, metadata);
            luceneField = new Field(ENTITY_ID_FIELD, id, // adding class
                    // namespace
                    Field.Store.YES, Field.Index.ANALYZED_NO_NORMS);
            document.add(luceneField);

            // index namespace for unique deletion
            luceneField = new Field(KUNDERA_ID_FIELD, getKunderaId(metadata, id), // adding
                    // class
                    // namespace
                    Field.Store.YES, Field.Index.ANALYZED_NO_NORMS);
            document.add(luceneField);

            // index entity class
            luceneField = new Field(ENTITY_CLASS_FIELD, metadata.getEntityClazz().getCanonicalName().toLowerCase(),
                    Field.Store.YES, Field.Index.ANALYZED_NO_NORMS);
            document.add(luceneField);

            // index index name
            luceneField = new Field(ENTITY_INDEXNAME_FIELD, metadata.getIndexName(), Field.Store.YES,
                    Field.Index.ANALYZED_NO_NORMS);
            document.add(luceneField);
        }
        catch (PropertyAccessException e)
        {
            throw new IllegalArgumentException("Id could not be read.");
        }
    }

    /**
     * Indexes document using Lucandra library
     * 
     * @param document
     *            the document
     */
    private void indexDocumentUsingLucandra(Document document)
    {
        try
        {
            RowMutation[] rms = null;
            lucandra.IndexWriter indexWriter = getIndexWriter();
            indexWriter.addDocument(INDEX_NAME, document, analyzer, docNumber++, true, rms);
        }
        catch (CorruptIndexException e)
        {
            throw new IndexingException(e.getMessage());
        }
        catch (IOException e)
        {
            throw new IndexingException(e.getMessage());
        }
    }

    /**
     * Indexes document in file system using lucene
     * 
     * @param document
     * @throws CorruptIndexException
     * @throws IOException
     */
    private void indexDocumentUsingLucene(Document document) throws CorruptIndexException, IOException
    {
        IndexWriter w = getDefaultIndexWriter();
        w.addDocument(document, analyzer);
        w.optimize();
        w.commit();
        w.close();
    }

    /**
     * Index field.
     * 
     * @param object
     *            the object
     * @param document
     *            the document
     * @param field
     *            the field
     * @param colName
     *            the col name
     * @param indexName
     *            the index name
     */
    private void indexField(Object object, Document document, java.lang.reflect.Field field, String colName,
            String indexName)
    {
        try
        {
            String value = PropertyAccessorHelper.getString(object, field);
            if (value != null)
            {
                Field luceneField = new Field(getCannonicalPropertyName(indexName, colName), value, Field.Store.NO,
                        Field.Index.ANALYZED);
                document.add(luceneField);
            }
            else
            {
                LOG.warn("value is null for field" + field.getName());
            }
        }
        catch (PropertyAccessException e)
        {
            LOG.error("Error in accessing field:" + e.getMessage());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.index.Indexer#search(java.lang.String, int, int)
     */
    @SuppressWarnings("deprecation")
    @Override
    public final Map<String, String> search(String luceneQuery, int start, int count)
    {

        if (Constants.INVALID == count)
        {
            count = 100;
        }

        LOG.debug("Searching index with query[" + luceneQuery + "], start:" + start + ", count:" + count);

        // Set<String> entityIds = new HashSet<String>();
        Map<String, String> indexCol = new HashMap<String, String>();

        org.apache.lucene.index.IndexReader indexReader = null;

        try
        {
            if (client.getType().equals(DBType.CASSANDRA))
            {
                indexReader = new IndexReader(INDEX_NAME);
            }
            else
            {
                indexReader = getDefaultReader();
            }
        }
        catch (Exception e)
        {
            throw new IndexingException(e.getMessage());
        }
        IndexSearcher searcher = new IndexSearcher(indexReader);

        QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, DEFAULT_SEARCHABLE_FIELD, analyzer);
        try
        {
            Query q = qp.parse(luceneQuery);
            TopDocs docs = searcher.search(q, count);

            int nullCount = 0;
            // Assuming Supercol will be null in case if alias only.
            // This is a quick fix
            for (ScoreDoc sc : docs.scoreDocs)
            {
                Document doc = searcher.doc(sc.doc);
                String entityId = doc.get(ENTITY_ID_FIELD);
                String superCol = doc.get(SUPERCOLUMN_INDEX);
                if (superCol == null)
                {
                    superCol = "SuperCol" + nullCount++;
                }
                indexCol.put(superCol, entityId);
            }
        }
        catch (ParseException e)
        {
            new IndexingException(e.getMessage());
        }
        catch (IOException e)
        {
            new IndexingException(e.getMessage());
        }

        // log.debug("Result[" + entityIds + "]");
        return indexCol;
    }

    /**
     * Gets the kundera id.
     * 
     * @param metadata
     *            the metadata
     * @param id
     *            the id
     * 
     * @return the kundera id
     */
    private String getKunderaId(EntityMetadata metadata, String id)
    {
        return metadata.getEntityClazz().getCanonicalName() + DELIMETER + id;
    }

    /**
     * Gets the cannonical property name.
     * 
     * @param indexName
     *            the index name
     * @param propertyName
     *            the property name
     * 
     * @return the cannonical property name
     */
    private String getCannonicalPropertyName(String indexName, String propertyName)
    {
        return indexName + "." + propertyName;
    }

    // helper method to get Lucandra IndexWriter object
    /**
     * Gets the index writer.
     * 
     * @return the index writer
     */
    private lucandra.IndexWriter getIndexWriter()
    {
        try
        {
            return new lucandra.IndexWriter();
        }
        catch (Exception e)
        {
            throw new IndexingException(e.getMessage());
        }
    }

    /**
     * Added for HBase support.
     * 
     * @return default index writer
     */
    private IndexWriter getDefaultIndexWriter()
    {
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
        Directory index = null;
        IndexWriter w = null;
        try
        {
            index = FSDirectory.open(getIndexDirectory());
            if (index.listAll().length == 0)
            {
                LOG.info("Creating fresh Index because it was empty");
                w = new IndexWriter(index, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);
            }
            else
            {
                w = new IndexWriter(index, analyzer, false, IndexWriter.MaxFieldLength.LIMITED);
            }

        }
        catch (CorruptIndexException e)
        {
            throw new IndexingException(e.getMessage());
        }
        catch (LockObtainFailedException e)
        {
            throw new IndexingException(e.getMessage());
        }
        catch (IOException e)
        {
            throw new IndexingException(e.getMessage());
        }
        return w;
    }

    /**
     * Returns default index reader.
     * 
     * @return index reader.
     */
    private org.apache.lucene.index.IndexReader getDefaultReader()
    {
        org.apache.lucene.index.IndexReader reader = null;
        try
        {
            reader = IndexReader.open(FSDirectory.open(getIndexDirectory()));
        }
        catch (CorruptIndexException e)
        {
            throw new IndexingException(e.getMessage());
        }
        catch (IOException e)
        {
            throw new IndexingException(e.getMessage());
        }
        return reader;
    }

    /**
     * Creates a directory if it does not exist.
     * 
     * @return the index directory
     */
    private File getIndexDirectory()
    {
        String filePath = System.getProperty("user.home") + "/lucene";
        File file = new File(filePath);
        if (!file.isDirectory())
        {
            file.mkdir();
        }
        return file;
    }
}
