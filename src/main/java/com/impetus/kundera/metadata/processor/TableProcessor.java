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
package com.impetus.kundera.metadata.processor;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;

import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Id;
import javax.persistence.Table;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.ejb.EntityManagerFactoryImpl;
import com.impetus.kundera.metadata.EntityMetadata;
import com.impetus.kundera.metadata.EntityMetadata.SuperColumn;
import com.impetus.kundera.metadata.EntityMetadata.Type;
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
            { // Whether @Embedded 
                metadata.setType(com.impetus.kundera.metadata.EntityMetadata.Type.SUPER_COLUMN_FAMILY);                
                Class embeddedFieldClass = f.getType();
                isEmbeddable = true;
                
                if(Collection.class.isAssignableFrom(embeddedFieldClass)) {
                    LOG.warn(f.getName() + " was annotated with @Embedded, and shouldn't have been a java Collection field, it won't be persisted");
                } else {
                    // An @Embedded attribute will be a DTO (@Embeddable)                     
                    populateEmbeddedFieldIntoMetadata(metadata, f, embeddedFieldClass);                   
                    metadata.addToEmbedCollection(embeddedFieldClass);  //TODO: Bad code, see how to remove this
                }
                
            }
            else if (f.isAnnotationPresent(ElementCollection.class))
            {
                //Whether a collection of embeddable objects
                metadata.setType(com.impetus.kundera.metadata.EntityMetadata.Type.SUPER_COLUMN_FAMILY);                
                Class elementCollectionFieldClass = f.getType();
                isEmbeddable = true;
                
                //An @ElementCollection must be a java collection, a generic class must be declared (@Embeddable)    
                if(Collection.class.isAssignableFrom(elementCollectionFieldClass)) {
                    Class elementCollectionGenericClass = PropertyAccessorHelper.getGenericClass(f);
                    populateElementCollectionIntoMetadata(metadata, f, elementCollectionGenericClass);
                    metadata.addToEmbedCollection(elementCollectionGenericClass);  //TODO: Bad code, see how to remove this
                } else {
                    LOG.warn(f.getName() + " was annotated with @ElementCollection but wasn't a java Collection field, it won't be persisted");
                }
            }
            else
            {
                // if any valid JPA annotation?
                String name = getValidJPAColumnName(clazz, f);
                if (null != name)
                {
                    // additional check for not to load Unnecessary column
                    // objects in JVM.
                    if (!isEmbeddable)
                    {
                        metadata.addColumn(name, metadata.new Column(name, f));
                    }
                    SuperColumn superColumn = metadata.new SuperColumn(name, f);
                    metadata.addSuperColumn(name, superColumn);

                    superColumn.addColumn(name, f);
                }
            }
        }
        
        if (isEmbeddable)
        {
            Map<String, EntityMetadata.Column> cols = metadata.getColumnsMap();
            cols.clear();
            cols = null;
            metadata.setType(Type.SUPER_COLUMN_FAMILY);
        }
        else
        {
            Map<String, EntityMetadata.SuperColumn> superColumns = metadata.getSuperColumnsMap();
            superColumns.clear();
            superColumns = null;
            metadata.setType(Type.COLUMN_FAMILY);
        }

    }

    private void populateEmbeddedFieldIntoMetadata(EntityMetadata metadata, Field embeddedField, Class embeddedFieldClass)
    {
        // If not annotated with @Embeddable, discard.
        Annotation ann = embeddedFieldClass.getAnnotation(Embeddable.class);
        if (ann == null)
        {
            LOG.warn(embeddedField.getName() + " was declared @Embedded but wasn't annotated with @Embeddable. "
                    + " It won't be persisted co-located.");
            //return;
        }

        // TODO: Provide user an option to specify this in entity class rather
        // than default field name
        String embeddedFieldName = embeddedField.getName();
        addSuperColumnInMetadata(metadata, embeddedField, embeddedFieldClass, embeddedFieldName);
    }
    
    private void populateElementCollectionIntoMetadata(EntityMetadata metadata, Field embeddedField, Class embeddedFieldClass)
    {
        // If not annotated with @Embeddable, discard.
        Annotation ann = embeddedFieldClass.getAnnotation(Embeddable.class);
        if (ann == null)
        {
            LOG.warn(embeddedField.getName() + " was declared @ElementCollection but wasn't annotated with @Embeddable. "
                    + " It won't be persisted co-located.");
            //return;
        }
        
        String embeddedFieldName = null;
        
        //Embedded object name should be the one provided with @CollectionTable annotation, if not, should be 
        //equal to field name
        if(embeddedField.isAnnotationPresent(CollectionTable.class)) {
            CollectionTable ct = embeddedField.getAnnotation(CollectionTable.class);
            if(!ct.name().isEmpty()) {
                embeddedFieldName = ct.name();
            } else {
                embeddedFieldName = embeddedField.getName();
            }
        } else {
            embeddedFieldName = embeddedField.getName();
        }       
        
        addSuperColumnInMetadata(metadata, embeddedField, embeddedFieldClass, embeddedFieldName);
    }

    /**
     * TODO: Change method name once we change the name "Super Column" in entity metadata
     * @param metadata
     * @param embeddedField
     * @param embeddedFieldClass
     * @param embeddedFieldName
     */
    private void addSuperColumnInMetadata(EntityMetadata metadata, Field embeddedField, Class embeddedFieldClass,
            String embeddedFieldName)
    {
        EntityMetadata.SuperColumn superColumn = metadata.getSuperColumn(embeddedFieldName);
        if (null == superColumn)
        {
            superColumn = metadata.new SuperColumn(embeddedFieldName, embeddedField);
        }
        // Iterate over all fields of this super column class
        for (Field columnField : embeddedFieldClass.getDeclaredFields())
        {    
            String columnName = getValidJPAColumnName(embeddedFieldClass, columnField);
            if(columnName == null) {
                columnName = columnField.getName();
            }             
            superColumn.addColumn(columnName, columnField);
        }
        metadata.addSuperColumn(embeddedFieldName, superColumn);
    }
}
