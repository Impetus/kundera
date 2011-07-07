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
/*
 * Copyright 2011 Impetus Infotech.
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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.List;

import javax.persistence.Embeddable;
import javax.persistence.Embedded;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Id;
import javax.persistence.Table;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jasper.tagplugins.jstl.core.Set;

import com.impetus.kundera.ejb.EntityManagerFactoryImpl;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Metadata processor class for persistent entities
 * 
 * @author amresh.singh
 */
public class TableProcessor extends AbstractEntityFieldProcessor
{

    /** The Constant log. */
    private static final Log LOG = LogFactory.getLog(TableProcessor.class);

    private EntityManagerFactoryImpl emf;

    /**
     * Instantiates a new table processor.
     * 
     * @param emf
     *            the emf
     */
    public TableProcessor(EntityManagerFactory emf)
    {
        this.emf = (EntityManagerFactoryImpl) emf;
    }

    @Override
    public void process(Class<?> clazz, EntityMetadata metadata)
    {
        if (!clazz.isAnnotationPresent(Table.class))
        {
            return;
        }

        LOG.debug("Processing @Entity(" + clazz.getName() + ") for Persistence Object.");
        populateMetadata(metadata, clazz);
    }

    private void populateMetadata(EntityMetadata metadata, Class<?> clazz)
    {
        Table table = clazz.getAnnotation(Table.class);
        boolean isEmbeddable = false;
        // Set Name of persistence object
        metadata.setTableName(table.name());

        // set database name
        String schema = table.schema().length() != 0 ? table.schema() : emf.getSchema();
        metadata.setSchema(schema);
        metadata.setType(com.impetus.kundera.metadata.EntityMetadata.Type.COLUMN_FAMILY);
        // scan for fields
        for (Field f : clazz.getDeclaredFields())
        {
            // Whether @Id field
            if (f.isAnnotationPresent(Id.class))
            {
                LOG.debug(f.getName() + " => Id");
                metadata.setIdProperty(f);
                populateIdAccessorMethods(metadata, clazz, f);
                populateIdColumn(metadata, clazz, f);
            }
            else if (f.isAnnotationPresent(Embedded.class))
            { // Whether @Embedded (only for Cassandra)
                metadata.setType(com.impetus.kundera.metadata.EntityMetadata.Type.SUPER_COLUMN_FAMILY);
                String superColumnName = f.getName();
                Class superColumnFieldClass = f.getType();

                // An embedded attribute can be either Collection or another DTO
                if (superColumnFieldClass.equals(List.class) || superColumnFieldClass.equals(Set.class))
                {
                    superColumnFieldClass = PropertyAccessorHelper.getGenericClass(f);
                    populateSuperColumnInMetadata(metadata, f, superColumnFieldClass);

                }
                else
                {
                    populateSuperColumnInMetadata(metadata, f, superColumnFieldClass);
                }
            }
            else
            {
                // if any valid JPA annotation?
                String name = getValidJPAColumnName(clazz, f);
                if (null != name)
                {
                    metadata.addColumn(name, metadata.new Column(name, f));
                }
            }
        }

    }

    private void populateSuperColumnInMetadata(EntityMetadata metadata, Field superColumnField, Class superColumnClass)
    {
        // If not annotated with @Embeddable, discard.
        Annotation ann = superColumnClass.getAnnotation(Embeddable.class);
        if (ann == null)
        {
            LOG.warn(superColumnClass
                    + " will not be persisted as Super Column because you didn't annotate it with @Embeddable");
            return;
        }

        // TODO: Provide user an option to specify this in entity class rather
        // than default field name
        String superColumnName = superColumnField.getName();
        EntityMetadata.SuperColumn superColumn = metadata.getSuperColumn(superColumnName);
        if (null == superColumn)
        {

            superColumn = metadata.new SuperColumn(superColumnName, superColumnField);
        }
        // Iterate over all fields of this super column class
        for (Field columnField : superColumnClass.getDeclaredFields())
        {
            String columnName = columnField.getName();
            superColumn.addColumn(columnName, columnField);
        }
        metadata.addSuperColumn(superColumnName, superColumn);
    }
}
