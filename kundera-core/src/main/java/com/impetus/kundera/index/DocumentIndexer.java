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

import java.io.CharArrayReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LetterTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(DocumentIndexer.class);

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

    /** The Constant PARENT_ID_FIELD. */
    public static final String PARENT_ID_FIELD = UUID + ".parent.id";

    /** The Constant PARENT_ID_CLASS. */
    public static final String PARENT_ID_CLASS = UUID + ".parent.class";

    /** The doc number. */
    protected static int docNumber = 1;

    /** The analyzer. */
    protected Analyzer analyzer;

    /** The tokenizer. */
    protected Tokenizer tokenizer;

    /**
     * Instantiates a new lucandra indexer.
     * 
     * @param analyzer
     *            the analyzer
     */
    public DocumentIndexer(Analyzer analyzer)
    {
        final String empty = "";
        this.analyzer = analyzer;
        tokenizer = new LetterTokenizer(Version.LUCENE_34, new CharArrayReader(empty.toCharArray()));
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
     * @param parentId
     *            the parent id
     * @param clazz
     *            the clazz
     * @return the document
     */
    protected Document prepareDocumentForSuperColumn(EntityMetadata metadata, Object object, String embeddedColumnName,
            String parentId, Class<?> clazz)
    {
        Document currentDoc;
        currentDoc = new Document();

        // Add entity class and row key info to document
        addEntityClassToDocument(metadata, object, currentDoc);

        // Add super column name to document
        addSuperColumnNameToDocument(embeddedColumnName, currentDoc);

        indexParentKey(parentId, currentDoc, clazz);
        return currentDoc;
    }

    /**
     * Index parent key.
     * 
     * @param parentId
     *            the parent id
     * @param currentDoc
     *            the current doc
     * @param clazz
     *            the clazz
     */
    protected void indexParentKey(String parentId, Document currentDoc, Class<?> clazz)
    {
        if (parentId != null)
        {
            Field luceneField = new Field(PARENT_ID_FIELD, parentId, Field.Store.YES, Field.Index.ANALYZED_NO_NORMS);
            currentDoc.add(luceneField);
            Field fieldClass = new Field(PARENT_ID_CLASS, clazz.getCanonicalName().toLowerCase(), Field.Store.YES,
                    Field.Index.ANALYZED);
            currentDoc.add(fieldClass);
        }
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
            EmbeddableType superColumn)
    {

        // Add all super column fields into document
        Set<Attribute> attributes = superColumn.getAttributes();
        Iterator<Attribute> iter = attributes.iterator();
        while (iter.hasNext())
        {
            Attribute attr = iter.next();
            java.lang.reflect.Field field = (java.lang.reflect.Field) attr.getJavaMember();
            String colName = field.getName();
            String indexName = metadata.getIndexName();
            addFieldToDocument(embeddedObject, currentDoc, field, colName, indexName);

        }
        // for (Column col : superColumn.getColumns())
        // {
        // java.lang.reflect.Field field = col.getField();
        // String colName = field.getName();
        // String indexName = metadata.getIndexName();
        // addFieldToDocument(embeddedObject, currentDoc, field, colName,
        // indexName);
        // }
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
    private void addSuperColumnNameToDocument(String superColumnName, Document currentDoc)
    {
        Field luceneField = new Field(SUPERCOLUMN_INDEX, superColumnName, Store.YES, Field.Index.NO);
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

        // for (PropertyIndex index : metadata.getIndexProperties())
        // {
        // java.lang.reflect.Field property = index.getProperty();
        // String propertyName = index.getName();
        // addFieldToDocument(object, document, property, propertyName,
        // indexName);
        // }

        Map<String, PropertyIndex> indexProperties = metadata.getIndexProperties();
        for (String columnName : indexProperties.keySet())
        {
            PropertyIndex index = indexProperties.get(columnName);
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
            Object id;
            id = PropertyAccessorHelper.getId(object, metadata);
            luceneField = new Field(ENTITY_ID_FIELD, id.toString(), Field.Store.YES, Field.Index.ANALYZED);
            // luceneField.set
            // adding class
            // namespace
            // /*Field.Store.YES, Field.Index.ANALYZED_NO_NORMS*/);
            document.add(luceneField);

            // index namespace for unique deletion
            luceneField = new Field(KUNDERA_ID_FIELD, getKunderaId(metadata, id), Field.Store.YES, Field.Index.NO); // adding
            // class
            // namespace
            // Field.Store.YES/*, Field.Index.ANALYZED_NO_NORMS*/);
            document.add(luceneField);

            // index entity class
            luceneField = new Field(ENTITY_CLASS_FIELD, metadata.getEntityClazz().getCanonicalName().toLowerCase(),
                    Field.Store.YES, Field.Index.ANALYZED);
            document.add(luceneField);
            //
            luceneField = new Field("timestamp", System.currentTimeMillis() + "", Field.Store.YES, Field.Index.NO);
            document.add(luceneField);

            // index index name
            luceneField = new Field(ENTITY_INDEXNAME_FIELD, metadata.getIndexName(), Field.Store.NO,
                    Field.Index.ANALYZED_NO_NORMS);
            document.add(luceneField);
        }
        catch (PropertyAccessException e)
        {
            throw new IllegalArgumentException("Id could not be read from object " + object);
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
            Object obj = PropertyAccessorHelper.getObject(object, field);
            // String value = (obj == null) ? null : obj.toString();
            if (obj != null)
            {
                Field luceneField = new Field(getCannonicalPropertyName(indexName, colName), obj.toString(),
                        Field.Store.YES, Field.Index.ANALYZED_NO_NORMS);
                document.add(luceneField);
            }
            else
            {
                LOG.warn("value is null for field" + field.getName());
            }
        }
        catch (PropertyAccessException e)
        {
            LOG.error("Error in accessing field, Caused by:" + e.getMessage());
            throw new LuceneIndexingException("Error in accessing field:" + field.getName(), e);
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
    protected String getKunderaId(EntityMetadata metadata, Object id)
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

    /**
     * Index document.
     * 
     * @param metadata
     *            the metadata
     * @param currentDoc
     *            the current doc
     */
    protected abstract void indexDocument(EntityMetadata metadata, Document currentDoc);

}
