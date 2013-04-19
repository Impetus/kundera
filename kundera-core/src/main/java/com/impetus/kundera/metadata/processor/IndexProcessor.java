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

import javax.persistence.Column;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.annotations.Index;
import com.impetus.kundera.index.IndexCollection;
import com.impetus.kundera.metadata.MetadataProcessor;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PropertyIndex;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;

/**
 * The Class BaseMetadataProcessor.
 * 
 * @author animesh.kumar
 */
public class IndexProcessor implements MetadataProcessor
{

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(IndexProcessor.class);

    public final void process(final Class<?> clazz, EntityMetadata metadata)
    {
        if (clazz != null)
        {
            metadata.setIndexName(clazz.getSimpleName());
        }
        Index idx = clazz.getAnnotation(Index.class);
        IndexCollection indexes = clazz.getAnnotation(IndexCollection.class);

        List<String> columnsNameToBeIndexed = new ArrayList<String>();

        Map<String, com.impetus.kundera.index.Index> indexedColumnsMap = new HashMap<String, com.impetus.kundera.index.Index>();

        if (null != indexes)
        {
            if (indexes.columns() != null && indexes.columns().length != 0)
            {
                metadata.setIndexable(true);

                for (com.impetus.kundera.index.Index indexedColumn : indexes.columns())
                {
                    indexedColumnsMap.put(indexedColumn.name(), indexedColumn);
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
        
        EntityType entityType  = (EntityType) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetaModelBuilder(metadata.getPersistenceUnit()).getManagedTypes().get(clazz);
        
        Set<Attribute> attributes = entityType.getAttributes();
        for(Attribute attrib : attributes)
        {
            if(!attrib.isAssociation())
            {
                String colName = attrib.getName();
                if (indexedColumnsMap != null && !indexedColumnsMap.isEmpty()
                        && indexedColumnsMap.containsKey(colName))
                {
                    com.impetus.kundera.index.Index indexedColumn = indexedColumnsMap.get(colName);
                    metadata.addIndexProperty(populatePropertyIndex(((AbstractAttribute)attrib).getJPAColumnName(), indexedColumn.type(),
                            indexedColumn.max(), indexedColumn.min(), (Field)attrib.getJavaMember()));
                    
                } else if (columnsNameToBeIndexed != null && !columnsNameToBeIndexed.isEmpty()
                        && columnsNameToBeIndexed.contains(colName))
                {
                    metadata.addIndexProperty(populatePropertyIndex(((AbstractAttribute)attrib).getJPAColumnName(), null, null, null, (Field)attrib.getJavaMember()));
                }
            }
            
        }
        /*
        for (Field f : clazz.getDeclaredFields())
        {
            if (f.isAnnotationPresent(Column.class))
            {
                String fieldName = f.getName();
                String colName = getIndexName(f, fieldName);
                if (indexedColumnsMap != null && !indexedColumnsMap.isEmpty()
                        && indexedColumnsMap.containsKey(fieldName))
                {
                    com.impetus.kundera.index.Index indexedColumn = indexedColumnsMap.get(fieldName);
                    metadata.addIndexProperty(populatePropertyIndex(indexedColumn.name(), indexedColumn.type(),
                            indexedColumn.max(), indexedColumn.min(), f));
                }
                else if (columnsNameToBeIndexed != null && !columnsNameToBeIndexed.isEmpty()
                        && columnsNameToBeIndexed.contains(colName))
                {
                    metadata.addIndexProperty(populatePropertyIndex(fieldName, null, null, null, f));
                }
            }
        }*/
    }

    /**
     * @param indexedColumn
     * @param f
     * @return TODO: Make this method accept n number of parameters elegantly
     */
    private static PropertyIndex populatePropertyIndex(String columnName, String indexType, Integer max, Integer min,
            Field f)
    {
        PropertyIndex pi = new PropertyIndex();

        pi.setProperty(f);
        pi.setName(columnName);
        pi.setIndexType(indexType);
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
     * Gets the index name.
     * 
     * @param f
     *            the f
     * @param alias
     *            the alias
     * @return the index name
     */
    private String getIndexName(Field f, String alias)
    {
        if (f.isAnnotationPresent(Column.class))
        {
            Column c = f.getAnnotation(Column.class);
            alias = c.name().trim();
            if (alias.isEmpty())
            {
                alias = f.getName();
            }
        }
        return alias;
    }
}
