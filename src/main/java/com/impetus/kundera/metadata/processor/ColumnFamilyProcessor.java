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

import javax.persistence.EntityManagerFactory;
import javax.persistence.Id;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.api.ColumnFamily;
import com.impetus.kundera.ejb.EntityManagerFactoryImpl;
import com.impetus.kundera.metadata.EntityMetadata;

/**
 * The Class ColumnFamilyMetadataProcessor.
 * 
 * @author animesh.kumar
 */
public class ColumnFamilyProcessor extends AbstractEntityFieldProcessor {

    /** The Constant log. */
    private static final Log LOG = LogFactory.getLog(ColumnFamilyProcessor.class);

    /** The em. */
    private EntityManagerFactoryImpl em;

    /**
     * Instantiates a new column family processor.
     * 
     * @param em
     *            the em
     */
    public ColumnFamilyProcessor(EntityManagerFactory em) {
        this.em = (EntityManagerFactoryImpl) em;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.metadata.MetadataProcessor#process(java.lang.Class,
     * com.impetus.kundera.metadata.EntityMetadata)
     */
    @Override
    public final void process(Class<?> clazz, EntityMetadata metadata) {

        if (!clazz.isAnnotationPresent(ColumnFamily.class)) {
            return;
        }

        LOG.debug("Processing @Entity(" + clazz.getName() + ") for ColumnFamily.");

        metadata.setType(EntityMetadata.Type.COLUMN_FAMILY);

        ColumnFamily cf = clazz.getAnnotation(ColumnFamily.class);

        // set columnFamily
        metadata.setColumnFamilyName(cf.family());

        // set keyspace
        String keyspace = cf.keyspace().length() != 0 ? cf.keyspace() : em.getKeyspace();
        metadata.setKeyspaceName(keyspace);

        // scan for fields
        for (Field f : clazz.getDeclaredFields()) {

            if (f.isAnnotationPresent(Id.class)) {
                LOG.debug(f.getName() + " => Id");
                metadata.setIdProperty(f);
            }

            else {
                String name = getValidJPAColumn(clazz, f);
                if (null != name) {
                    metadata.addColumn(name, metadata.new Column(name, f));
                }
            }

        }
    }
}
