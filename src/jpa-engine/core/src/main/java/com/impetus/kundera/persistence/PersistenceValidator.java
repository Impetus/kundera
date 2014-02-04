/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.persistence;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Set;

import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.metadata.processor.MetaModelBuilder;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.validation.ValidationFactory;
import com.impetus.kundera.validation.ValidationFactoryGenerator;
import com.impetus.kundera.validation.ValidationFactoryGenerator.ValidationFactoryType;
import com.impetus.kundera.validation.rules.AttributeConstraintRule;

/**
 * Responsible for validating entity persistence
 * 
 * @author amresh.singh
 */
public class PersistenceValidator
{
    private static final Logger log = LoggerFactory.getLogger(PersistenceValidator.class);

    /**
     * Validates an entity object for CRUD operations
     * 
     * @param entity
     *            Instance of entity object
     * @return True if entity object is valid, false otherwise
     */
    public boolean isValidEntityObject(Object entity, EntityMetadata metadata)
    {
        if (entity == null)
        {
            log.error("Entity to be persisted must not be null, operation failed");
            return false;
        }

        Object id = PropertyAccessorHelper.getId(entity, metadata);
        if (id == null)
        {
            log.error("Entity to be persisted can't have Primary key set to null.");
            throw new IllegalArgumentException("Entity to be persisted can't have Primary key set to null.");
            // return false;
        }
        return true;
    }

    /**
     * Validates an entity object for CRUD operations
     * 
     * @param entity
     *            Instance of entity object
     */
    public void validate(Object entity, KunderaMetadata kunderaMetadata)
    {
        validateEntityAttributes(entity, kunderaMetadata);
    }

    /**
     * Validates an entity object for CRUD operations
     * 
     * @param entity
     *            Instance of entity object
     */
    public <X extends Class, T extends Object> void validateEntityAttributes(Object entity,
            KunderaMetadata kunderaMetadata)
    {
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, entity.getClass());

        MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                entityMetadata.getPersistenceUnit());
        AbstractManagedType managedType = (AbstractManagedType) metaModel.entity(entityMetadata.getEntityClazz());

        if (managedType.isValidateConstraints())
        {
            MetaModelBuilder<X, T> metaModelBuilder = kunderaMetadata.getApplicationMetadata().getMetaModelBuilder(
                    entityMetadata.getPersistenceUnit());
            EntityType entityType = (EntityType) metaModelBuilder.getManagedTypes()
                    .get(entityMetadata.getEntityClazz());

            Set<Attribute> attributes = entityType.getAttributes();

            Iterator<Attribute> iter = attributes.iterator();
            ValidationFactoryGenerator generator = new ValidationFactoryGenerator();
            ValidationFactory factory = generator.getFactory(ValidationFactoryType.BOOT_STRAP_VALIDATION);
            while (iter.hasNext())
            {
                Attribute attribute = iter.next();

                Field f = (Field) ((Field) attribute.getJavaMember());
                factory.validate(f, entity, new AttributeConstraintRule());

            }

        }
    }
}
