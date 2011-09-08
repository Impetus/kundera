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
package com.impetus.kundera.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.classreading.AnnotationDiscoveryListener;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.processor.CacheableAnnotationProcessor;
import com.impetus.kundera.metadata.processor.EntityListenersProcessor;
import com.impetus.kundera.metadata.processor.IndexProcessor;
import com.impetus.kundera.metadata.processor.TableProcessor;
import com.impetus.kundera.metadata.validator.EntityValidator;
import com.impetus.kundera.metadata.validator.EntityValidatorImpl;

/**
 * Concrete implementation of IMetadataManager.
 *
 * @author animesh.kumar
 */
public class MetadataManager implements AnnotationDiscoveryListener
{

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(MetadataManager.class);

    /** cache for Metadata. */
    private Map<Class<?>, EntityMetadata> metadataCache = new ConcurrentHashMap<Class<?>, EntityMetadata>();

    /** The entity name to class map. */
    private Map<String, Class<?>> entityNameToClassMap = new ConcurrentHashMap<String, Class<?>>();

    /** The metadata processors. */
    private List<MetadataProcessor> metadataProcessors;

    /** The Validator. */
    private EntityValidator validator;

    // set after build is called?
    /** The instantiated. */
    private boolean instantiated = false;

    /**
     * Instantiates a new metadata manager.
     *
     * @param factory
     *            the factory
     */
    public MetadataManager()
    {
        validator = new EntityValidatorImpl();
        metadataProcessors = new ArrayList<MetadataProcessor>();

        // add processors to chain.
        metadataProcessors.add(new TableProcessor());
        metadataProcessors.add(new CacheableAnnotationProcessor());
        metadataProcessors.add(new IndexProcessor());
        metadataProcessors.add(new EntityListenersProcessor());
    } 
    
    /**
     * Validate.
     *
     * @param clazz
     *            the clazz
     *
     * @throws PersistenceException
     *             the persistence exception
     */
    public final void validate(Class<?> clazz) throws PersistenceException
    {
        validator.validate(clazz);
    }

    /**
     * Gets the entity metadata.
     *
     * @param clazz
     *            the clazz
     *
     * @return the entity metadata
     *
     * @throws PersistenceException
     *             the persistence exception
     */
    public final EntityMetadata getEntityMetadata(Class<?> clazz)
    {

        EntityMetadata metadata = metadataCache.get(clazz);
        if (null == metadata)
        {
            log.debug("Metadata not found in cache for " + clazz.getName());
            // double check locking.
            synchronized (clazz)
            {
                if (null == metadata)
                {
                    metadata = buildEntityMetadata(clazz);
                    cacheMetadata(clazz, metadata);
                }
            }
        }
        return metadata;
    }

    // helper methods to strip CGLIB from class

    /**
     * Process.
     *
     * @param clazz
     *            the clazz
     *
     * @return the entity metadata
     *
     * @throws PersistenceException
     *             the persistence exception
     */
    public EntityMetadata buildEntityMetadata(Class<?> clazz)
    {

        EntityMetadata metadata = new EntityMetadata(clazz);
        validate(clazz);

        log.debug("Processing @Entity >> " + clazz);

        for (MetadataProcessor processor : metadataProcessors)
        {
            processor.process(clazz, metadata);
        }

        return metadata;
    }

    /**
     * Cache metadata.
     *
     * @param clazz
     *            the clazz
     * @param metadata
     *            the metadata
     */
    private void cacheMetadata(Class<?> clazz, EntityMetadata metadata)
    {
        metadataCache.put(clazz, metadata);

        // save name to class map.
        if (entityNameToClassMap.containsKey(clazz.getSimpleName()))
        {
            throw new PersistenceException("Name conflict between classes "
                    + entityNameToClassMap.get(clazz.getSimpleName()).getName() + " and " + clazz.getName());
        }
        entityNameToClassMap.put(clazz.getSimpleName(), clazz);
    }

    /**
     * Gets the entity class by name.
     *
     * @param name
     *            the name
     *
     * @return the entity class by name
     */
    public final Class<?> getEntityClassByName(String name)
    {
        return entityNameToClassMap.get(name);
    }

    /**
     * Gets the entity metadatas as list.
     *
     * @return the entity metadatas as list
     */
    public final List<EntityMetadata> getEntityMetadatasAsList()
    {
        return Collections.unmodifiableList(new ArrayList<EntityMetadata>(metadataCache.values()));
    }

    /*
     * @see
     * com.impetus.kundera.classreading.AnnotationDiscoveryListener#discovered
     * (java.lang.String, java.lang.String[])
     */
    @Override
    // called whenever a class with @Entity annotation is encountered in the
    // classpath.
    public final void discovered(String className, String[] annotations)
    {
        try
        {
            Class<?> clazz = Class.forName(className);

            // process for Metadata
            EntityMetadata metadata = buildEntityMetadata(clazz);
            cacheMetadata(clazz, metadata);
            log.info("Added @Entity " + clazz.getName());
        }
        catch (ClassNotFoundException e)
        {
            throw new PersistenceException(e.getMessage());
        }
    }

    /**
     * Build Inter/Intra @Entity relationships.
     */
    /*public void build()
    {
        log.debug("Building @Entity's foreign relations.");
        for (EntityMetadata metadata : getEntityMetadatasAsList())
        {
            processRelations(metadata.getEntityClazz());
            log.debug("Metadata for @Entity " + metadata.getEntityClazz() + "\n" + metadata);
        }
        instantiated = true;
    }*/
    
}
