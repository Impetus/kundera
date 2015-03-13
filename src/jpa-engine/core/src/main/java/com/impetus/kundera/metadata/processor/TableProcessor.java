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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javassist.Modifier;

import javax.persistence.ElementCollection;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;
import javax.persistence.NamedNativeQueries;
import javax.persistence.NamedNativeQuery;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.PersistenceException;
import javax.persistence.Transient;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.SingularAttribute;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.loader.MetamodelLoaderException;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata.Type;
import com.impetus.kundera.metadata.model.type.AbstractIdentifiableType;
import com.impetus.kundera.metadata.processor.relation.RelationMetadataProcessor;
import com.impetus.kundera.metadata.processor.relation.RelationMetadataProcessorFactory;
import com.impetus.kundera.metadata.validator.EntityValidatorImpl;
import com.impetus.kundera.metadata.validator.InvalidEntityDefinitionException;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.validation.ValidationFactory;
import com.impetus.kundera.validation.ValidationFactoryGenerator;
import com.impetus.kundera.validation.ValidationFactoryGenerator.ValidationFactoryType;
import com.impetus.kundera.validation.rules.RelationAttributeRule;
import com.impetus.kundera.validation.rules.RuleValidationException;

/**
 * Metadata processor class for persistent entities.
 * 
 * @author amresh.singh
 */
public class TableProcessor extends AbstractEntityFieldProcessor
{

    /** The Constant log. */
    private static final Logger LOG = LoggerFactory.getLogger(TableProcessor.class);

    /** holds pu prperties */
    private Map puProperties;

    private ValidationFactory factory;

    /**
     * Instantiates a new table processor.
     */
    public TableProcessor(Map puProperty, KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata);
        validator = new EntityValidatorImpl(puProperty);
        // validator = new EntityValidatorImpl(puProperty);
        ValidationFactoryGenerator generator = new ValidationFactoryGenerator();
        this.factory = generator.getFactory(ValidationFactoryType.BOOT_STRAP_VALIDATION);
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
        // process for metamodelImpl

        if (metadata.getPersistenceUnit() != null)
        {
            MetaModelBuilder<X, T> metaModelBuilder = kunderaMetadata.getApplicationMetadata().getMetaModelBuilder(
                    metadata.getPersistenceUnit());

            onBuildMetaModelSuperClass(clazz.getSuperclass(), metaModelBuilder);

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
                }
            }

            EntityType entityType = (EntityType) metaModelBuilder.getManagedTypes().get(metadata.getEntityClazz());

            validateAndSetId(metadata, clazz, metaModelBuilder);
            validateandSetEntityType(metadata, clazz, metaModelBuilder);
            MetadataUtils.onJPAColumnMapping(entityType, metadata);

            /* Scan for Relationship field */
            populateRelationMetaData(entityType, clazz, metadata);
        }
    }

    /**
     * Populate metadata.
     * 
     * @param entityType
     *            the EntityType
     * @param <X>
     *            the generic type
     * @param metadata
     *            the metadata
     * @throws RuleValidationException
     */
    private <X> void populateRelationMetaData(EntityType entityType, Class<X> clazz, EntityMetadata metadata)
    {
        Set<Attribute> attributes = entityType.getAttributes();

        for (Attribute attribute : attributes)
        {
            if (attribute.isAssociation())
            {

                addRelationIntoMetadata(clazz, (Field) attribute.getJavaMember(), metadata);
            }
        }

    }

    /**
     * Populate metadata.
     * 
     * @param <X>
     *            the generic type
     * @param <T>
     *            the generic type
     * @param metaModelBuilder
     *            the metaModelBuilder
     */
    private <X, T> void onBuildMetaModelSuperClass(Class<? super X> clazz, MetaModelBuilder<X, T> metaModelBuilder)
    {
        if (clazz != null && clazz.isAnnotationPresent(javax.persistence.Entity.class))
        {
            while (clazz != null && clazz.isAnnotationPresent(javax.persistence.Entity.class))
            {
                metaModelBuilder.process((Class<X>) clazz);

                for (Field f : clazz.getDeclaredFields())
                {
                    if (f != null && !Modifier.isStatic(f.getModifiers()) && !Modifier.isTransient(f.getModifiers())
                            && !f.isAnnotationPresent(Transient.class))
                    {
                        metaModelBuilder.construct((Class<X>) clazz, f);
                    }

                }
                clazz = clazz.getSuperclass();
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
            relProcessor = RelationMetadataProcessorFactory
                    .getRelationMetadataProcessor(relationField, kunderaMetadata);

            this.factory.validate(relationField, new RelationAttributeRule());

            relProcessor = RelationMetadataProcessorFactory
                    .getRelationMetadataProcessor(relationField, kunderaMetadata);

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
        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();
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
            populateIdAccessorMethods(entityMetadata, clazz, f);
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
            if ((f.isAnnotationPresent(Embedded.class) && f.getType().getAnnotation(Embeddable.class) != null))
            {
                entityMetadata.setType(Type.SUPER_COLUMN_FAMILY);
            }
            else if (f.isAnnotationPresent(ElementCollection.class) && !MetadataUtils.isBasicElementCollectionField(f))
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
     * 
     * @param metadata
     * @param clazz
     * @param metaModelBuilder
     */
    private <X, T> void validateAndSetId(EntityMetadata metadata, Class<X> clazz,
            MetaModelBuilder<X, T> metaModelBuilder)
    {
        if (metadata.getIdAttribute() == null)
        {
            EntityType entityType = (EntityType) metaModelBuilder.getManagedTypes().get(clazz);

            if (entityType.getSupertype() != null)
            {
                Attribute idAttribute = ((AbstractIdentifiableType) entityType.getSupertype()).getIdAttribute();

                metadata.setIdAttribute((SingularAttribute) idAttribute);
                populateIdAccessorMethods(metadata, clazz, (Field) idAttribute.getJavaMember());
            }
        }

        validateIdAttribute(metadata.getIdAttribute(), clazz);
    }
    
    /**
     * 
     * @param metadata
     * @param clazz
     * @param metaModelBuilder
     */
    private <X, T> void validateandSetEntityType(EntityMetadata metadata, Class<X> clazz,
            MetaModelBuilder<X, T> metaModelBuilder)
    {
        if (metadata.getType() == null && clazz != null && !clazz.equals(Object.class) 
                            && clazz.isAnnotationPresent(javax.persistence.Entity.class))
        {
            EntityType entityType = (EntityType) metaModelBuilder.getManagedTypes().get(clazz);

            if (entityType.getSupertype() != null)
            {
                Set<Attribute> attributes = ((AbstractIdentifiableType) entityType.getSupertype()).getAttributes();
                Iterator<Attribute> iter = attributes.iterator();

                while (iter.hasNext())
                {
                    Attribute attribute = iter.next();
                   
                    Field f = (Field) ((Field) attribute.getJavaMember());
                    
                    onFamilyType(metadata, clazz, f);
                }
                

            }
            validateandSetEntityType(metadata, (Class<X>) clazz.getSuperclass(), metaModelBuilder);
        }

    }

    /**
     * 
     * @param idAttribute
     * @param clazz
     */
    private void validateIdAttribute(SingularAttribute idAttribute, Class clazz)
    {
        // Means if id attribute not found neither on entity or mappedsuper
        // class.

        if (idAttribute == null)
        {
            throw new InvalidEntityDefinitionException(clazz.getName() + " must have an @Id field.");
        }
    }

}