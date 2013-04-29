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
import java.util.Map;

import javassist.Modifier;

import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.NamedNativeQueries;
import javax.persistence.NamedNativeQuery;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.PersistenceException;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.SingularAttribute;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.loader.MetamodelLoaderException;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata.Type;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.metadata.processor.relation.RelationMetadataProcessor;
import com.impetus.kundera.metadata.processor.relation.RelationMetadataProcessorFactory;
import com.impetus.kundera.metadata.validator.EntityValidatorImpl;
import com.impetus.kundera.metadata.validator.InvalidEntityDefinitionException;

/**
 * Metadata processor class for persistent entities.
 * 
 * @author amresh.singh
 */
public class TableProcessor extends AbstractEntityFieldProcessor
{

    /** The Constant log. */
    private static final Log LOG = LogFactory.getLog(TableProcessor.class);

    /** holds pu prperties */
    private Map puProperties;

    /**
     * Instantiates a new table processor.
     */
    public TableProcessor(Map puProperty)
    {
        validator = new EntityValidatorImpl(puProperty);
        this.puProperties = puProperty;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.metadata.MetadataProcessor#process(java.lang.Class,
     * com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public void process(Class clazz, EntityMetadata metadata)
    {

        if (LOG.isDebugEnabled())
            LOG.debug("Processing @Entity(" + clazz.getName() + ") for Persistence Object.");
        populateMetadata(metadata, clazz, puProperties);

    }

    /**
     * Populate metadata.
     * 
     * @param <X>
     *            the generic type
     * @param <T>
     *            the generic type
     * @param metadata
     *            the metadata
     * @param clazz
     *            the clazz
     */
    private <X extends Class, T extends Object> void populateMetadata(EntityMetadata metadata, Class<X> clazz,
            Map puProperties)
    {
        Table table = clazz.getAnnotation(Table.class);
        boolean isEmbeddable = false;
        // Set Name of persistence object
        metadata.setTableName(table.name());
        // Add named/native query related application metadata.
        addNamedNativeQueryMetadata(clazz);
        // set schema name and persistence unit name (if provided)
        String schemaStr = table.schema();

        if (schemaStr == null)
        {
            LOG.error("It is mandatory to specify Schema alongwith Table name:" + table.name()
                    + ". This entity won't be persisted");
            throw new InvalidEntityDefinitionException("It is mandatory to specify Schema alongwith Table name:"
                    + table.name() + ". This entity won't be persisted");
        }
        MetadataUtils.setSchemaAndPersistenceUnit(metadata, schemaStr, puProperties);

        // scan for fields

        // process for metamodelImpl

        if (metadata.getPersistenceUnit() != null)
        {
            MetaModelBuilder<X, T> metaModelBuilder = KunderaMetadata.INSTANCE.getApplicationMetadata()
                    .getMetaModelBuilder(metadata.getPersistenceUnit());
            metaModelBuilder.process(clazz);

            for (Field f : clazz.getDeclaredFields())
            {
                if (f != null && !Modifier.isStatic(f.getModifiers()) && !Modifier.isTransient(f.getModifiers())
                        && !f.isAnnotationPresent(Transient.class))
                {
                    // construct metamodel.
                    metaModelBuilder.construct(clazz, f);

                    // on id attribute.

                    onIdAttribute(metaModelBuilder, metadata, clazz, f);

                    // determine if it is a column family or super column
                    // family.

                    onFamilyType(metadata, clazz, f);

                    onJPAColumnMapping(metaModelBuilder, metadata, f);

                    /* Scan for Relationship field */
                    addRelationIntoMetadata(clazz, f, metadata);
                }
            }
        }

    }

    /**
     * Adds relationship info into metadata for a given field
     * <code>relationField</code>.
     * 
     * @param entityClass
     *            the entity class
     * @param relationField
     *            the relation field
     * @param metadata
     *            the metadata
     */
    private void addRelationIntoMetadata(Class<?> entityClass, Field relationField, EntityMetadata metadata)
    {
        RelationMetadataProcessor relProcessor = null;

        try
        {

            relProcessor = RelationMetadataProcessorFactory.getRelationMetadataProcessor(relationField);

            if (relProcessor != null)
            {
                relProcessor.addRelationIntoMetadata(relationField, metadata);
            }

        }
        catch (PersistenceException pe)
        {
            throw new MetamodelLoaderException("Error with relationship in @Entity(" + entityClass + "."
                    + relationField.getName() + "), reason: " + pe);
        }
    }

    /**
     * Add named/native query annotated fields to application meta data.
     * 
     * @param clazz
     *            entity class.
     */
    private void addNamedNativeQueryMetadata(Class clazz)
    {
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        String name, query = null;
        if (clazz.isAnnotationPresent(NamedQuery.class))
        {
            NamedQuery ann = (NamedQuery) clazz.getAnnotation(NamedQuery.class);
            appMetadata.addQueryToCollection(ann.name(), ann.query(), false, clazz);
        }

        if (clazz.isAnnotationPresent(NamedQueries.class))
        {
            NamedQueries ann = (NamedQueries) clazz.getAnnotation(NamedQueries.class);

            NamedQuery[] anns = ann.value();
            for (NamedQuery a : anns)
            {
                appMetadata.addQueryToCollection(a.name(), a.query(), false, clazz);
            }
        }

        if (clazz.isAnnotationPresent(NamedNativeQuery.class))
        {
            NamedNativeQuery ann = (NamedNativeQuery) clazz.getAnnotation(NamedNativeQuery.class);
            appMetadata.addQueryToCollection(ann.name(), ann.query(), true, clazz);
        }

        if (clazz.isAnnotationPresent(NamedNativeQueries.class))
        {
            NamedNativeQueries ann = (NamedNativeQueries) clazz.getAnnotation(NamedNativeQueries.class);

            NamedNativeQuery[] anns = ann.value();
            for (NamedNativeQuery a : anns)
            {
                appMetadata.addQueryToCollection(a.name(), a.query(), true, clazz);
            }
        }
    }

    /**
     * On id attribute.
     * 
     * @param builder
     *            the builder
     * @param entityMetadata
     *            the entity metadata
     * @param clazz
     *            the clazz
     * @param f
     *            the f
     */
    private void onIdAttribute(final MetaModelBuilder builder, EntityMetadata entityMetadata, final Class clazz, Field f)
    {
        EntityType entity = (EntityType) builder.getManagedTypes().get(clazz);
        Attribute attrib = entity.getAttribute(f.getName());
        if (!attrib.isCollection() && ((SingularAttribute) attrib).isId())
        {
            entityMetadata.setIdAttribute((SingularAttribute) attrib);

            // populateIdAccessorMethods(entityMetadata, clazz, f);
        }
    }

    /**
     * On family type.
     * 
     * @param entityMetadata
     *            the entity metadata
     * @param clazz
     *            the clazz
     * @param f
     *            the f
     */
    private void onFamilyType(EntityMetadata entityMetadata, final Class clazz, Field f)
    {
        if (entityMetadata.getType() == null || !entityMetadata.getType().equals(Type.SUPER_COLUMN_FAMILY))
        {
            if (f.isAnnotationPresent(Embedded.class) || f.isAnnotationPresent(ElementCollection.class))
            {
                entityMetadata.setType(Type.SUPER_COLUMN_FAMILY);
            }
            else
            {
                entityMetadata.setType(Type.COLUMN_FAMILY);
            }
        }
    }

    /**
     * On jpa column mapping.
     * 
     * @param builder
     *            the builder
     * @param entityMetadata
     *            the entity metadata
     * @param f
     *            the f
     */
    private void onJPAColumnMapping(final MetaModelBuilder builder, EntityMetadata entityMetadata, Field f)
    {
        EntityType entityType = (EntityType) builder.getManagedTypes().get(entityMetadata.getEntityClazz());
        AbstractAttribute attribute = (AbstractAttribute) entityType.getAttribute(f.getName());
        entityMetadata.addJPAColumnMapping(attribute.getJPAColumnName(), f.getName());
    }

}
