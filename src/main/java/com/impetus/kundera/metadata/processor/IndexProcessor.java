/*
 * Copyright 2010 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.metadata.processor;

import java.lang.reflect.Field;

import javax.persistence.Column;
import javax.persistence.Id;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.api.Index;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.MetadataProcessor;

/**
 * The Class BaseMetadataProcessor.
 * 
 * @author animesh.kumar
 */
public class IndexProcessor implements MetadataProcessor {

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(IndexProcessor.class);

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.metadata.MetadataProcessor#process(java.lang.Class,
     * com.impetus.kundera.metadata.EntityMetadata)
     */
    public final void process(final Class<?> clazz, EntityMetadata metadata) {

        metadata.setIndexName(clazz.getSimpleName());

        Index idx = clazz.getAnnotation(Index.class);
        if (null != idx) {
            boolean isIndexable = idx.index();
            metadata.setIndexable(isIndexable);

            if (!isIndexable) {
                log.debug("@Entity " + clazz.getName() + " will not be indexed.");
                return;
            }
        }

        log.debug("Processing @Entity " + clazz.getName() + " for Indexes.");

        // scan for fields
        for (Field f : clazz.getDeclaredFields()) {
            if (f.isAnnotationPresent(Column.class)) {
                Column c = f.getAnnotation(Column.class);
                String alias = c.name().trim();
                if (alias.isEmpty()) {
                    alias = f.getName();
                }

                metadata.addIndexProperty(metadata.new PropertyIndex(f, alias));

            } else if (f.isAnnotationPresent(Id.class)) {
                metadata.addIndexProperty(metadata.new PropertyIndex(f, f.getName()));
            }
        }
    }
}
