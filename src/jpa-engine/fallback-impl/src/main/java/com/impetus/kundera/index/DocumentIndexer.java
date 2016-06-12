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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Id;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PropertyIndex;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.model.Relation.ForeignKey;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * The Class KunderaIndexer.
 * 
 * @author animesh.kumar
 */
@SuppressWarnings(value = { "all" })
public abstract class DocumentIndexer implements com.impetus.kundera.index.lucene.Indexer {

    /** log for this class. */
    private static final Logger LOG = LoggerFactory.getLogger(DocumentIndexer.class);

    /** The INDEX_NAME. */
    protected static final String INDEX_NAME = "kundera-alpha";// is

    /** The Constant UUID. */
    private static final long UUID = 6077004083174677888L;

    /** The Constant DEFAULT_SEARCHABLE_FIELD. */
    protected static final String DEFAULT_SEARCHABLE_FIELD = UUID + ".default_property";

    /** The Constant SUPERCOLUMN_INDEX. */
    protected static final String SUPERCOLUMN_INDEX = UUID + ".entity.super.indexname";

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
    public DocumentIndexer() {
        final String empty = "";
        this.analyzer = new StandardAnalyzer();
        tokenizer = new LetterTokenizer();
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
        String parentId, Class<?> clazz) {
        Document currentDoc;
        currentDoc = new Document();

        // Add entity class and row key info to document
        addEntityClassToDocument(metadata, object, currentDoc, null);

        // Add super column name to document
        addSuperColumnNameToDocument(embeddedColumnName, currentDoc);

        addParentKeyToDocument(parentId, currentDoc, clazz);
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
    protected void addParentKeyToDocument(String parentId, Document currentDoc, Class<?> clazz) {
        // if (parentId != null)
        if (clazz != null && parentId != null) {
            Field luceneField =
                new Field(IndexingConstants.PARENT_ID_FIELD, parentId, Field.Store.YES, Field.Index.ANALYZED_NO_NORMS);
            currentDoc.add(luceneField);
            Field fieldClass =
                new Field(IndexingConstants.PARENT_ID_CLASS, clazz.getCanonicalName().toLowerCase(), Field.Store.YES,
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
     * @param metamodel
     */
    protected void createSuperColumnDocument(EntityMetadata metadata, Object object, Document currentDoc,
        Object embeddedObject, EmbeddableType superColumn, MetamodelImpl metamodel) {

        // Add all super column fields into document
        Set<Attribute> attributes = superColumn.getAttributes();
        Iterator<Attribute> iter = attributes.iterator();
        while (iter.hasNext()) {
            Attribute attr = iter.next();
            java.lang.reflect.Field field = (java.lang.reflect.Field) attr.getJavaMember();
            String colName = field.getName();
            String indexName = metadata.getIndexName();
            addFieldToDocument(embeddedObject, currentDoc, field, colName, indexName);
        }

        // Add all entity fields to document
        addEntityFieldsToDocument(metadata, object, currentDoc, metamodel);

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
     * @param metamodel
     */
    protected void indexSuperColumn(EntityMetadata metadata, Object object, Document currentDoc, Object embeddedObject,
        EmbeddableType superColumn, MetamodelImpl metamodel) {

        // Add all super column fields into document
        Set<Attribute> attributes = superColumn.getAttributes();
        Iterator<Attribute> iter = attributes.iterator();
        while (iter.hasNext()) {
            Attribute attr = iter.next();
            java.lang.reflect.Field field = (java.lang.reflect.Field) attr.getJavaMember();
            String colName = field.getName();
            String indexName = metadata.getIndexName();
            addFieldToDocument(embeddedObject, currentDoc, field, colName, indexName);
        }

        // Add all entity fields to document
        addEntityFieldsToDocument(metadata, object, currentDoc, metamodel);

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
    private void addSuperColumnNameToDocument(String superColumnName, Document currentDoc) {
        Field luceneField = new Field(SUPERCOLUMN_INDEX, superColumnName, Store.YES, Field.Index.NO);
        currentDoc.add(luceneField);
    }

    /**
     * Adds the index properties.
     * 
     * @param metadata
     *            the metadata
     * @param entity
     *            the object
     * @param document
     *            the document
     * @param metaModel
     */
    protected void addEntityFieldsToDocument(EntityMetadata metadata, Object entity, Document document,
        MetamodelImpl metaModel) {
        String indexName = metadata.getIndexName();
        Map<String, PropertyIndex> indexProperties = metadata.getIndexProperties();
        for (String columnName : indexProperties.keySet()) {
            PropertyIndex index = indexProperties.get(columnName);
            java.lang.reflect.Field property = index.getProperty();
            String propertyName = index.getName();
            addFieldToDocument(entity, document, property, propertyName, indexName);
        }

        if (metaModel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType())) {
            Object id = PropertyAccessorHelper.getId(entity, metadata);
            EmbeddableType embeddableId = metaModel.embeddable(metadata.getIdAttribute().getBindableJavaType());
            Set<Attribute> embeddedAttributes = embeddableId.getAttributes();
            indexCompositeKey(embeddedAttributes, metadata, id, document, metaModel);
        }
    }

    /**
     * @param metadata
     * @param entity
     * @param document
     * @param metaModel
     * 
     *            Add indexes for associated columns
     */
    protected void addAssociatedEntitiesToDocument(EntityMetadata metadata, Object entity, Document document,
        MetamodelImpl metaModel) {
        try {
            IndexCollection indexes = metadata.getEntityClazz().getAnnotation(IndexCollection.class);
            if (indexes != null) {
                List<String> columnsNameToBeIndexed = new ArrayList<String>();
                for (com.impetus.kundera.index.Index indexedColumn : indexes.columns()) {
                    Attribute attrib = metaModel.getEntityAttribute(entity.getClass(), indexedColumn.name());
                    columnsNameToBeIndexed.add(((AbstractAttribute) attrib).getJPAColumnName());
                }

                String indexName = metadata.getIndexName();
                List<Relation> relations = metadata.getRelations();
                for (Relation relation : relations) {
                    if (relation.getType().equals(ForeignKey.MANY_TO_ONE)
                        || relation.getType().equals(ForeignKey.ONE_TO_ONE)) {
                        String propertyName = relation.getJoinColumnName(null);
                        if (propertyName != null && columnsNameToBeIndexed.contains(propertyName)) {
                            java.lang.reflect.Field property = relation.getProperty();
                            Object obj = PropertyAccessorHelper.getObject(entity, property);
                            if (obj != null) {
                                EntityMetadata relMetaData = metaModel.getEntityMetadata(obj.getClass());
                                Object id = PropertyAccessorHelper.getId(obj, relMetaData);
                                if (id != null) {
                                    Field luceneField =
                                        new Field(getCannonicalPropertyName(indexName, propertyName), id.toString(),
                                            Field.Store.YES, Field.Index.ANALYZED_NO_NORMS);

                                    document.add(luceneField);
                                } else {
                                    LOG.warn("value is null for field" + property.getName());
                                }
                            }
                        }
                    }
                }
            }
        } catch (PropertyAccessException e) {
            LOG.error("Error in accessing field, Caused by:" + e.getMessage());
            throw new LuceneIndexingException("Error in creating indexes on associated columns", e);
        }
    }

    /**
     * index compositekey
     * 
     * @param metadata
     * @param id
     * @param document
     * @param metaModel
     */
    protected void indexCompositeKey(Set<Attribute> embeddedAttributes, EntityMetadata metadata, Object id,
        Document document, final MetamodelImpl metaModel) {
        // indexing individual fields of the composite key
        try {
            for (Attribute attribute : embeddedAttributes) {
                if (!ReflectUtils.isTransientOrStatic((java.lang.reflect.Field) attribute.getJavaMember())) {
                    if (metaModel.isEmbeddable(attribute.getJavaType())) {
                        EmbeddableType embeddable = metaModel.embeddable(attribute.getJavaType());
                        indexCompositeKey(embeddable.getAttributes(), metadata,
                            ((java.lang.reflect.Field) attribute.getJavaMember()).get(id), document, metaModel);
                    } else {
                        String columnName = ((AbstractAttribute) attribute).getJPAColumnName();
                        addFieldToDocument(id, document, (java.lang.reflect.Field) attribute.getJavaMember(),
                            columnName, metadata.getEntityClazz().getSimpleName());
                    }
                }
            }
        } catch (IllegalAccessException e) {
            LOG.error(e.getMessage());
        }
    }

    /**
     * Prepare index document.
     * 
     * @param metadata
     *            the metadata
     * @param entity
     *            the object
     * @param document
     *            the document
     */
    protected void addEntityClassToDocument(EntityMetadata metadata, Object entity, Document document,
        final MetamodelImpl metaModel) {
        try {
            Field luceneField;
            Object id;
            id = PropertyAccessorHelper.getId(entity, metadata);

            // Indexing composite keys
            if (metaModel != null && metaModel.isEmbeddable(metadata.getIdAttribute().getBindableJavaType())) {
                id = KunderaCoreUtils.prepareCompositeKey(metadata.getIdAttribute(), metaModel, id);
            }

            luceneField =
                new Field(IndexingConstants.ENTITY_ID_FIELD, id.toString(), Field.Store.YES, Field.Index.ANALYZED);

            // luceneField.set
            // adding class
            // namespace
            // /*Field.Store.YES, Field.Index.ANALYZED_NO_NORMS*/);
            document.add(luceneField);

            // index namespace for unique deletion
            luceneField =
                new Field(IndexingConstants.KUNDERA_ID_FIELD, getKunderaId(metadata, id), Field.Store.YES,
                    Field.Index.ANALYZED); // adding
            // class
            // namespace
            // Field.Store.YES/*, Field.Index.ANALYZED_NO_NORMS*/);
            document.add(luceneField);

            // index entity class
            luceneField =
                new Field(IndexingConstants.ENTITY_CLASS_FIELD, metadata.getEntityClazz().getCanonicalName()
                    .toLowerCase(), Field.Store.YES, Field.Index.ANALYZED);
            document.add(luceneField);
            //
            luceneField = new Field("timestamp", System.currentTimeMillis() + "", Field.Store.YES, Field.Index.NO);
            document.add(luceneField);

            // index index name
            luceneField =
                new Field(IndexingConstants.ENTITY_INDEXNAME_FIELD, metadata.getIndexName(), Field.Store.NO,
                    Field.Index.ANALYZED_NO_NORMS);
            document.add(luceneField);

            luceneField =
                new Field(getCannonicalPropertyName(metadata.getEntityClazz().getSimpleName(),
                    ((AbstractAttribute) metadata.getIdAttribute()).getJPAColumnName()), id.toString(),
                    Field.Store.YES, Field.Index.ANALYZED_NO_NORMS);
            document.add(luceneField);
        } catch (PropertyAccessException e) {
            throw new IllegalArgumentException("Id could not be read from object " + entity);
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
        String indexName) {
        try {
            Object obj = PropertyAccessorHelper.getObject(object, field);
            // String str =
            // String value = (obj == null) ? null : obj.toString();
            if (obj != null) {
                Field luceneField =
                    new Field(getCannonicalPropertyName(indexName, colName), obj.toString(), Field.Store.YES,
                        Field.Index.ANALYZED_NO_NORMS);

                document.add(luceneField);
            } else {
                LOG.warn("value is null for field" + field.getName());
            }
        } catch (PropertyAccessException e) {
            LOG.error("Error in accessing field, Caused by:" + e.getMessage());
            throw new LuceneIndexingException("Error in accessing field:" + field.getName(), e);
        }
    }

    @Override
    public Map<String, Object> search(String query, Class<?> parentClass, EntityMetadata parentMetadata,
        Class<?> childClass, EntityMetadata childMetadata, Object entityId, int start, int count) {
        return null;
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
    protected String getKunderaId(EntityMetadata metadata, Object id) {
        return metadata.getEntityClazz().getCanonicalName() + IndexingConstants.DELIMETER + id;
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
    protected String getCannonicalPropertyName(String indexName, String propertyName) {
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