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

import java.io.CharArrayReader;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LetterTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.Version;

import com.impetus.kundera.metadata.model.Column;
import com.impetus.kundera.metadata.model.EmbeddedColumn;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.PropertyIndex;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * The Class KunderaIndexer.
 * 
 * @author animesh.kumar
 */
public abstract class DocumentIndexer implements Indexer
{

    /** log for this class. */
    private static final Log LOG = LogFactory.getLog(DocumentIndexer.class);

    /** The INDEX_NAME. */
    protected static final String INDEX_NAME = "kundera-alpha";// is

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
    protected static final String DEFAULT_SEARCHABLE_FIELD = UUID + ".default_property";

    /** The Constant SUPERCOLUMN_INDEX. */
    protected static final String SUPERCOLUMN_INDEX = UUID + ".entity.super.indexname";

    /** The doc number. */
    protected static int docNumber = 1;

    /** The analyzer. */
    protected Analyzer analyzer;

    protected Tokenizer tokenizer;

    /**
     * Instantiates a new lucandra indexer.
     * 
     * @param client
     *            the client
     * @param analyzer
     *            the analyzer
     */
    public DocumentIndexer(Analyzer analyzer)
    {
        final String empty = "";
        this.analyzer = analyzer;
        tokenizer = new LetterTokenizer(Version.LUCENE_34,new CharArrayReader(empty.toCharArray()));
    }

    /**
     * Prepare document.
     * 
     * @param metadata
     *            the metadata
     * @param object
     *            the object
     * @param embeddedColumnName
     *            the super column name
     * @return the document
     */
    protected Document prepareDocumentForSuperColumn(EntityMetadata metadata, Object object, String embeddedColumnName)
    {
        Document currentDoc;
        currentDoc = new Document();

        // Add entity class and row key info to document
        addEntityClassToDocument(metadata, object, currentDoc);

        // Add super column name to document
        addEmbeddedColumnNameToDocument(embeddedColumnName, currentDoc);
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
    protected void indexSuperColumn(EntityMetadata metadata, Object object, Document currentDoc, Object embeddedObject,
            EmbeddedColumn superColumn)
    {
        // Add all super column fields into document
        for (Column col : superColumn.getColumns())
        {
            java.lang.reflect.Field field = col.getField();
            String colName = field.getName();
            String indexName = metadata.getIndexName();
            addFieldToDocument(embeddedObject, currentDoc, field, colName, indexName);
        }
        // Add all entity fields to document
        addEntityFieldsToDocument(metadata, object, currentDoc);

        // Store document into Index
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
    private void addEmbeddedColumnNameToDocument(String superColumnName, Document currentDoc)
    {        
        Field luceneField = getLuceneField(SUPERCOLUMN_INDEX, superColumnName);
        currentDoc.add(luceneField);

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
    protected void addEntityFieldsToDocument(EntityMetadata metadata, Object object, Document document)
    {
        String indexName = metadata.getIndexName();
        for (PropertyIndex index : metadata.getIndexProperties())
        {

            java.lang.reflect.Field property = index.getProperty();
            String propertyName = index.getName();
            addFieldToDocument(object, document, property, propertyName, indexName);
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
    protected void addEntityClassToDocument(EntityMetadata metadata, Object object, Document document)
    {
        try
        {

            Field luceneField;
            String id;
            id = PropertyAccessorHelper.getId(object, metadata);            
            //luceneField = new Field(ENTITY_ID_FIELD, value, 0, value.length);
            luceneField = getLuceneField(ENTITY_ID_FIELD, id);
            document.add(luceneField);

            // index namespace for unique deletion            
            luceneField = getLuceneField(KUNDERA_ID_FIELD, getKunderaId(metadata, id));
            document.add(luceneField);

            // index entity class
            luceneField = getLuceneField(ENTITY_CLASS_FIELD, metadata.getEntityClazz().getCanonicalName().toLowerCase());
            document.add(luceneField);

            // index index name            
            luceneField = getLuceneField(ENTITY_INDEXNAME_FIELD, metadata.getIndexName());
            document.add(luceneField);
        }
        catch (PropertyAccessException e)
        {
            throw new IllegalArgumentException("Id could not be read.");
        }
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
    private void addFieldToDocument(Object object, Document document, java.lang.reflect.Field field, String colName,
            String indexName)
    {
        try
        {
            String value = PropertyAccessorHelper.getString(object, field);
            if (value != null)
            {
                Field luceneField = getLuceneField(getCannonicalPropertyName(indexName, colName), value);
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
    protected String getKunderaId(EntityMetadata metadata, String id)
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

    protected abstract void indexDocument(EntityMetadata metadata, Document currentDoc);
    
    
    public Field getLuceneField(String name, String value) {
//        try
//        {
  //          tokenizer = new StandardTokenizer(Version.LUCENE_34,new CharArrayReader(value.toCharArray()));
//            tokenizer.reset(new CharArrayReader(value.toCharArray()));
          //  return new Field(name, tokenizer);        
//        }
//        catch (IOException e)
//        {
//            throw new IndexingException(e.getMessage());
//        }
        return new Field(name, value, Field.Store.YES, Field.Index.ANALYZED_NO_NORMS);
    }

}
