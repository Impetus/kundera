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

import javax.persistence.Column;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.annotations.Index;
import com.impetus.kundera.metadata.MetadataProcessor;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.PropertyIndex;
import com.impetus.kundera.newannotations.IndexCollection;

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

        List<String> columnsNameToBeIndexed;
        Map<String, com.impetus.kundera.newannotations.Index> columnsToBeIndexed = null;
        if (null != indexes)
        {
            columnsToBeIndexed = new HashMap<String, com.impetus.kundera.newannotations.Index>();
            // metadata.setIndexName(clazz.getSimpleName());
            if (indexes.columns() != null && indexes.columns().length != 0)
            {
                metadata.setIndexable(true);

                for (com.impetus.kundera.newannotations.Index indexedColumn : indexes.columns())
                {
                    columnsToBeIndexed.put(indexedColumn.name(), indexedColumn);
                }
                // metadata.setColToBeIndexed(columnsToBeIndexed);
            }
        }
        if (null != idx)
        {
            columnsNameToBeIndexed = new ArrayList<String>();
            boolean isIndexable = idx.index();
            metadata.setIndexable(isIndexable);

            String indexName = idx.name();
            if (indexName != null && !indexName.isEmpty())
            {
                metadata.setIndexName(indexName);
            }
            // else
            // {
            // metadata.setIndexName(clazz.getSimpleName());
            // }

            if (idx.columns() != null && idx.columns().length != 0)
            {
                for (String indexedColumn : idx.columns())
                {
                    columnsNameToBeIndexed.add(indexedColumn);
                }
                // metadata.setColToBeIndexed(columnsToBeIndexed);
            }
            //
            // if (!isIndexable)
            // {
            // log.debug("@Entity " + clazz.getName() +
            // " will not be indexed for "
            // + (columnsToBeIndexed.isEmpty() ? "all columns" :
            // columnsToBeIndexed));
            // return;
            // }
        }
        else
        {
            log.debug("@Entity " + clazz.getName() + " will not be indexed for "
                    + (columnsToBeIndexed.isEmpty() ? "all columns" : columnsToBeIndexed));
            return;
        }

        log.debug("Processing @Entity " + clazz.getName() + " for Indexes.");

        // scan for fields
        for (Field f : clazz.getDeclaredFields())
        {
            if (f.isAnnotationPresent(Column.class))
            {
                String fieldName = f.getName();
                // fieldName = getIndexName(f, fieldName);

                if (columnsToBeIndexed != null && !columnsToBeIndexed.isEmpty()
                        && columnsToBeIndexed.containsKey(fieldName))
                {
                    com.impetus.kundera.newannotations.Index indexedColumn = columnsToBeIndexed.get(fieldName);
                    metadata.addIndexProperty(populatePropertyIndex(indexedColumn.name(), indexedColumn.type(),
                            indexedColumn.max(), indexedColumn.max(), f));
                }
                else if (columnsNameToBeIndexed != null && !columnsNameToBeIndexed.isEmpty()
                        && columnsNameToBeIndexed.contains(fieldName))
                {
                    metadata.addIndexProperty(populatePropertyIndex(fieldName, null, null, null, f));
                }
            }
        }
    }

    /**
     * @param indexedColumn
     * @param f
     * @return
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

    /**
     * Returns list of indexed columns
     * 
     * @param entityMetadata
     *            entity metadata
     * @return list of indexed columns
     */
    public static Map<String, PropertyIndex> getIndexesOfEmbeddable(Class<?> entityClazz)
    {
        Map<String, PropertyIndex> pis = new HashMap<String, PropertyIndex>();
        Index idx = entityClazz.getAnnotation(Index.class);
        IndexCollection indexes = entityClazz.getAnnotation(IndexCollection.class);
        List<String> columnsNameToBeIndexed = null;
        Map<String, com.impetus.kundera.newannotations.Index> columnsToBeIndexed = null;
        if (null != indexes)
        {
            columnsToBeIndexed = new HashMap<String, com.impetus.kundera.newannotations.Index>();
            if (indexes.columns() != null && indexes.columns().length != 0)
            {
                for (com.impetus.kundera.newannotations.Index indexedColumn : indexes.columns())
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
            List<String> columnsNameToBeIndexed,
            Map<String, com.impetus.kundera.newannotations.Index> columnsToBeIndexed)
    {
        for (Field f : entityClazz.getDeclaredFields())
        {
            if (f.isAnnotationPresent(Column.class))
            {
                String fieldName = f.getName();
                if (columnsToBeIndexed != null && !columnsToBeIndexed.isEmpty()
                        && columnsToBeIndexed.containsKey(fieldName))
                {
                    com.impetus.kundera.newannotations.Index indexedColumn = columnsToBeIndexed.get(fieldName);
                    pis.put(indexedColumn.name(),
                            populatePropertyIndex(indexedColumn.name(), indexedColumn.type(), indexedColumn.max(),
                                    indexedColumn.max(), f));
                }
                else if (columnsNameToBeIndexed != null && !columnsNameToBeIndexed.isEmpty()
                        && columnsNameToBeIndexed.contains(fieldName))
                {
                    pis.put(fieldName, populatePropertyIndex(fieldName, null, null, null, f));
                }
            }
        }
    }

}
