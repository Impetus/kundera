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
package com.impetus.kundera.metadata.validator;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.TableGenerator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.client.ClientResolver;
import com.impetus.kundera.configure.schema.api.SchemaManager;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * Validates entity for JPA rules.
 * 
 * @author animesh.kumar
 */
public class EntityValidatorImpl implements EntityValidator
{

    /** The Constant log. */
    private static final Log log = LogFactory.getLog(EntityValidatorImpl.class);

    /** cache for validated classes. */
    private List<Class<?>> classes = new ArrayList<Class<?>>();

    private Map<String, Object> puProperties;

    /**
     * @param puPropertyMap
     */
    public EntityValidatorImpl(Map puPropertyMap)
    {
        this.puProperties = puPropertyMap;
    }

    /**
     * @param externalPropertyMap
     */
    public EntityValidatorImpl()
    {
        this(null);
    }

    /**
     * Checks the validity of a class for Cassandra entity.
     * 
     * @param clazz
     *            validates this class
     * 
     * @return returns 'true' if valid
     */
    @Override
    // TODO: reduce Cyclomatic complexity
    public final void validate(final Class<?> clazz)
    {

        if (classes.contains(clazz))
        {
            return;
        }

        if (log.isDebugEnabled())
            log.debug("Validating " + clazz.getName());

        // Is Entity?
        if (!clazz.isAnnotationPresent(Entity.class))
        {
            throw new InvalidEntityDefinitionException(clazz.getName() + " is not annotated with @Entity");
        }

        // Must be annotated with @Table
        if (!clazz.isAnnotationPresent(Table.class))
        {
            throw new InvalidEntityDefinitionException(clazz.getName() + " must be annotated with @Table");
        }

        // must have a default no-argument constructor
        try
        {
            clazz.getConstructor();
        }
        catch (NoSuchMethodException nsme)
        {
            throw new InvalidEntityDefinitionException(clazz.getName()
                    + " must have a default no-argument constructor.");
        }

        // Check for @Key and ensure that there is just 1 @Key field of String
        // type.
        List<Field> keys = new ArrayList<Field>();
        for (Field field : clazz.getDeclaredFields())
        {
            if (field.isAnnotationPresent(Id.class) && field.isAnnotationPresent(EmbeddedId.class))
            {
                throw new InvalidEntityDefinitionException(clazz.getName()
                        + " must have either @Id field or @EmbeddedId field");
            }

            if (field.isAnnotationPresent(Id.class))
            {
                keys.add(field);
                // validate @GeneratedValue annotation if given
                if (field.isAnnotationPresent(GeneratedValue.class))
                {
                    validateGeneratedValueAnnotation(clazz, field);
                }
            }
            else if (field.isAnnotationPresent(EmbeddedId.class))
            {
                keys.add(field);
            }
        }

        if (keys.size() < 0)
        {
            throw new InvalidEntityDefinitionException(clazz.getName() + " must have an @Id field.");
        }
        else if (keys.size() > 1)
        {
            throw new InvalidEntityDefinitionException(clazz.getName() + " can only have 1 @Id field.");
        }

        // if (!keys.get(0).getType().equals(String.class))
        // {
        // throw new PersistenceException(clazz.getName() +
        // " @Id must be of String type.");
        // }

        // save in cache

        classes.add(clazz);
    }

    private void validateGeneratedValueAnnotation(final Class<?> clazz, Field field)
    {
        Table table = clazz.getAnnotation(Table.class);
        String schemaName = table.schema();
        if (schemaName != null && schemaName.indexOf('@') > 0)
        {
            schemaName = schemaName.substring(0, schemaName.indexOf('@'));
            GeneratedValue generatedValue = field.getAnnotation(GeneratedValue.class);
            if (generatedValue != null && generatedValue.generator() != null && !generatedValue.generator().isEmpty())
            {
                if (!(field.isAnnotationPresent(TableGenerator.class) || field.isAnnotationPresent(SequenceGenerator.class)
                        || clazz.isAnnotationPresent(TableGenerator.class) || clazz
                            .isAnnotationPresent(SequenceGenerator.class)))
                {
                    throw new IllegalArgumentException("Unknown Id.generator: " + generatedValue.generator());
                }
                else
                {
                    checkForGenerator(clazz, field, generatedValue, schemaName);
                }
            }
        }        
    }

    private void checkForGenerator(final Class<?> clazz, Field field, GeneratedValue generatedValue, String schemaName)
    {
        TableGenerator tableGenerator = field.getAnnotation(TableGenerator.class);
        SequenceGenerator sequenceGenerator = field.getAnnotation(SequenceGenerator.class);
        if (tableGenerator == null || !tableGenerator.name().equals(generatedValue.generator()))
        {
            tableGenerator = clazz.getAnnotation(TableGenerator.class);
        }
        if (sequenceGenerator == null || !sequenceGenerator.name().equals(generatedValue.generator()))
        {
            sequenceGenerator = clazz.getAnnotation(SequenceGenerator.class);
        }

        if ((tableGenerator == null && sequenceGenerator == null)
                || (tableGenerator != null && !tableGenerator.name().equals(generatedValue.generator()))
                || (sequenceGenerator != null && !sequenceGenerator.name().equals(generatedValue.generator())))
        {
            throw new IllegalArgumentException("Unknown Id.generator: " + generatedValue.generator());
        }
        else if ((tableGenerator != null && !tableGenerator.schema().isEmpty() && !tableGenerator.schema().equals(
                schemaName))
                || (sequenceGenerator != null && !sequenceGenerator.schema().isEmpty() && !sequenceGenerator.schema()
                        .equals(schemaName)))
        {
            throw new InvalidEntityDefinitionException("Generator " + generatedValue.generator() + " in entity : "
                    + clazz.getName() + " has different schema name ,it should be same as entity have");
        }
    }

    @Override
    public void validateEntity(Class<?> clazz)
    {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(clazz);
        if (metadata != null)
        {
            SchemaManager schemaManager = ClientResolver.getClientFactory(metadata.getPersistenceUnit(), puProperties)
                    .getSchemaManager(puProperties);
            if (schemaManager != null && !schemaManager.validateEntity(clazz))
            {
                log.warn("Validation for : " + clazz + " failed , any operation on this class will result in fail.");
            }
        }
    }
}
