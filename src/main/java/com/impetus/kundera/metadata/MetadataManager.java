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
import java.util.List;

import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
public class MetadataManager
{

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(MetadataManager.class);

    
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
