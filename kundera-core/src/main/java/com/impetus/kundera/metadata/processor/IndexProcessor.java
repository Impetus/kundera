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
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Id;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.annotations.Index;
import com.impetus.kundera.annotations.IndexedColumn;
import com.impetus.kundera.metadata.MetadataProcessor;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.PropertyIndex;

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
        metadata.setIndexName(clazz.getSimpleName());
        Index idx = clazz.getAnnotation(Index.class);
        // List<String> columnsToBeIndexed = new ArrayList<String>();
        Map<String, IndexedColumn> columnsToBeIndexed = new HashMap<String, IndexedColumn>();

        if (null != idx)
        {
            boolean isIndexable = idx.index();
            metadata.setIndexable(isIndexable);

            String indexName = idx.name();
            if (indexName != null && !indexName.isEmpty())
            {
                metadata.setIndexName(indexName);
            }
            else
            {
                metadata.setIndexName(clazz.getSimpleName());
            }

            if (idx.indexedColumns() != null && idx.indexedColumns().length != 0)
            {
                for (IndexedColumn indexedColumn : idx.indexedColumns())
                {
                    columnsToBeIndexed.put(indexedColumn.name(), indexedColumn);
                }
                metadata.setColToBeIndexed(columnsToBeIndexed);
            }

            if (!isIndexable)
            {
                log.debug("@Entity " + clazz.getName() + " will not be indexed for "
                        + (columnsToBeIndexed.isEmpty() ? "all columns" : columnsToBeIndexed));
                return;
            }
        }

        log.debug("Processing @Entity " + clazz.getName() + " for Indexes.");

        // scan for fields
        for (Field f : clazz.getDeclaredFields())
        {
            if (f.isAnnotationPresent(Column.class))
            {
                String fieldName = f.getName();
//                fieldName = getIndexName(f, fieldName);

                if (!columnsToBeIndexed.isEmpty() && columnsToBeIndexed.containsKey(fieldName))
                {
                    metadata.addIndexProperty(columnsToBeIndexed.get(fieldName), f);
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
