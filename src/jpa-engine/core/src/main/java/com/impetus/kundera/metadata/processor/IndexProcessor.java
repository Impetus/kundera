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
package com.impetus.kundera.metadata.processor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.persistence.Column;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.annotations.Index;
import com.impetus.kundera.index.IndexCollection;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.PropertyIndex;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * The Class BaseMetadataProcessor.
 * 
 * @author animesh.kumar
 */
public class IndexProcessor extends AbstractEntityFieldProcessor
{

    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(IndexProcessor.class);

    public IndexProcessor(KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata);
    }

    public final void process(final Class<?> clazz, EntityMetadata metadata)
    {
        if (clazz != null)
        {
            metadata.setIndexName(clazz.getSimpleName());
        }
        Index idx = clazz.getAnnotation(Index.class);

        IndexCollection indexes = clazz.getAnnotation(IndexCollection.class);

        EntityType entityType = (EntityType) kunderaMetadata.getApplicationMetadata()
                .getMetaModelBuilder(metadata.getPersistenceUnit()).getManagedTypes().get(clazz);

        List<String> columnsNameToBeIndexed = new ArrayList<String>();

        Map<String, com.impetus.kundera.index.Index> indexedColumnsMap = new HashMap<String, com.impetus.kundera.index.Index>();

        if (null != indexes)
        {
            if (indexes.columns() != null && indexes.columns().length != 0)
            {
                metadata.setIndexable(true);
                for (com.impetus.kundera.index.Index indexedColumn : indexes.columns())
                {
                    if (indexedColumn.type().equals("composite"))
                    {
                        // means comma seperated list of columns
                        metadata.addIndexProperty(
                                prepareCompositeIndexName(indexedColumn.name(), entityType),
                                populatePropertyIndex(indexedColumn.indexName(), indexedColumn.type(), null, null, null));

                    }
                    else
                    {
                        indexedColumnsMap.put(indexedColumn.name(), indexedColumn);
                    }
                }
            }
        }
        else if (null != idx)
        {
            boolean isIndexable = idx.index();

            if (isIndexable)
            {
                metadata.setIndexable(isIndexable);

                String indexName = idx.name();
                if (indexName != null && !indexName.isEmpty())
                {
                    metadata.setIndexName(indexName);
                }

                if (idx.columns() != null && idx.columns().length != 0)
                {
                    for (String indexedColumn : idx.columns())
                    {
                        columnsNameToBeIndexed.add(indexedColumn);
                    }
                }
            }
        }
        else
        {
            log.debug("@Entity " + clazz.getName() + " will not be indexed for "
                    + (indexedColumnsMap.isEmpty() ? "all columns" : indexedColumnsMap));
            return;
        }

        log.debug("Processing @Entity " + clazz.getName() + " for Indexes.");

        // scan for fields
        Set<Attribute> attributes = entityType.getAttributes();
        for (Attribute attrib : attributes)
        {
            if (!attrib.isAssociation())
            {
                String colName = attrib.getName();
                String columnName = ((AbstractAttribute) attrib).getJPAColumnName();
                if (indexedColumnsMap != null && !indexedColumnsMap.isEmpty() && indexedColumnsMap.containsKey(colName))
                {
                    com.impetus.kundera.index.Index indexedColumn = indexedColumnsMap.get(colName);
                    String indexName = StringUtils.isBlank(indexedColumn.indexName()) ? columnName : indexedColumn
                            .indexName();
                    metadata.addIndexProperty(
                            columnName,
                            populatePropertyIndex(indexName, indexedColumn.type(), indexedColumn.max(),
                                    indexedColumn.min(), (Field) attrib.getJavaMember()));

                }
                else if (columnsNameToBeIndexed != null && !columnsNameToBeIndexed.isEmpty()
                        && columnsNameToBeIndexed.contains(colName))
                {
                    metadata.addIndexProperty(columnName,
                            populatePropertyIndex(columnName, null, null, null, (Field) attrib.getJavaMember()));
                }
            }

        }
    }

    /**
     * @param indexedColumn
     * @param f
     * @return TODO: Make this method accept n number of parameters elegantly
     */
    private static PropertyIndex populatePropertyIndex(String indexName, String indexType, Integer max, Integer min,
            Field f)
    {
        PropertyIndex pi = new PropertyIndex(f, indexName, indexType);

        pi.setMax(max);
        pi.setMin(min);

        return pi;
    }

    /**
     * Returns list of indexed columns on {@code @Embeddable} entity
     * 
     * @param entityMetadata
     *            entity metadata
     * @return list of indexed columns
     */
    public static Map<String, PropertyIndex> getIndexesOnEmbeddable(Class<?> entityClazz)
    {
        Map<String, PropertyIndex> pis = new HashMap<String, PropertyIndex>();
        Index idx = entityClazz.getAnnotation(Index.class);
        IndexCollection indexes = entityClazz.getAnnotation(IndexCollection.class);
        List<String> columnsNameToBeIndexed = null;
        Map<String, com.impetus.kundera.index.Index> columnsToBeIndexed = null;
        if (null != indexes)
        {
            columnsToBeIndexed = new HashMap<String, com.impetus.kundera.index.Index>();
            if (indexes.columns() != null && indexes.columns().length != 0)
            {
                for (com.impetus.kundera.index.Index indexedColumn : indexes.columns())
                {
                    columnsToBeIndexed.put(indexedColumn.name(), indexedColumn);
                }
            }
        }
        if (null != idx)
        {
            columnsNameToBeIndexed = new ArrayList<String>();
            if (idx.columns() != null && idx.columns().length != 0)
            {
                for (String indexedColumn : idx.columns())
                {
                    columnsNameToBeIndexed.add(indexedColumn);
                }
            }
        }
        getPropertyIndexes(entityClazz, pis, columnsNameToBeIndexed, columnsToBeIndexed);
        return pis;
    }

    /**
     * @param entityClazz
     * @param pis
     * @param columnsNameToBeIndexed
     * @param columnsToBeIndexed
     */
    private static void getPropertyIndexes(Class<?> entityClazz, Map<String, PropertyIndex> pis,
            List<String> columnsNameToBeIndexed, Map<String, com.impetus.kundera.index.Index> columnsToBeIndexed)
    {
        for (Field f : entityClazz.getDeclaredFields())
        {
            if (f.isAnnotationPresent(Column.class))
            {
                String fieldName = f.getName();
                if (columnsToBeIndexed != null && !columnsToBeIndexed.isEmpty()
                        && columnsToBeIndexed.containsKey(fieldName))
                {
                    com.impetus.kundera.index.Index indexedColumn = columnsToBeIndexed.get(fieldName);
                    pis.put(indexedColumn.name(),
                            populatePropertyIndex(indexedColumn.name(), indexedColumn.type(), indexedColumn.max(),
                                    indexedColumn.min(), f));
                }
                else if (columnsNameToBeIndexed != null && !columnsNameToBeIndexed.isEmpty()
                        && columnsNameToBeIndexed.contains(fieldName))
                {
                    pis.put(fieldName, populatePropertyIndex(fieldName, null, null, null, f));
                }
            }
        }
    }

    /**
     * prepare composite index.
     * 
     * @param indexedColumns
     * @param entityType
     * @return
     */
    private String prepareCompositeIndexName(String indexedColumns, final EntityType entityType)
    {
        StringTokenizer tokenizer = new StringTokenizer(indexedColumns, ",");
        StringBuilder builder = new StringBuilder();
        while (tokenizer.hasMoreTokens())
        {
            String fieldName = (String) tokenizer.nextElement();
            builder.append(((AbstractAttribute) entityType.getAttribute(fieldName)).getJPAColumnName());
            builder.append(",");
        }

        builder.deleteCharAt(builder.length() - 1);

        return builder.toString();
    }
}
