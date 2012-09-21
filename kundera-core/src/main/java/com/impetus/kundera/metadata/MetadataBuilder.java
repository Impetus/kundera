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
package com.impetus.kundera.metadata;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.PersistenceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.Constants;
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
public class MetadataBuilder
{

    /** the log used by this class. */
    private static Log log = LogFactory.getLog(MetadataBuilder.class);

    /** The metadata processors. */
    private List<MetadataProcessor> metadataProcessors;

    /** The Validator. */
    private EntityValidator validator;

    // set after build is called?
    /** The instantiated. */
    private boolean instantiated = false;

    /** persistence unit */
    private String persistenceUnit;

    /** kundera client */
    private String client;

    /**
     * Instantiates a new metadata manager.
     * 
     */

    public MetadataBuilder(String puName, String client)
    {
        this.persistenceUnit = puName;
        this.client = client;
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
     * @return the entity metadata
     */
    public EntityMetadata buildEntityMetadata(Class<?> clazz)
    {

        EntityMetadata metadata = new EntityMetadata(clazz);
        validate(clazz);

        log.debug("Processing @Entity >> " + clazz);

        for (MetadataProcessor processor : metadataProcessors)
        {
            // in case it is not intend for current persistence unit.
            checkForRDBMS(metadata);
            processor.process(clazz, metadata);
            metadata = belongsToPersistenceUnit(metadata);
            if (metadata == null)
            {
                break;
            }
        }

        return metadata;
    }

    private void checkForRDBMS(EntityMetadata metadata)
    {
        if (Constants.RDBMS_CLIENT_FACTORY.equalsIgnoreCase(client))
        {
            // no more "null" as persistence unit for RDBMS scenarios!
            metadata.setPersistenceUnit(persistenceUnit);
        }
    }

    /**
     * If parameterised metadata is not for intended persistence unit, assign it
     * to null.
     * 
     * @param metadata
     *            entity metadata
     * @return metadata.
     */
    private EntityMetadata belongsToPersistenceUnit(EntityMetadata metadata)
    {
        // if pu is null and client is not rdbms OR metadata pu does not match
        // with configured one. don't process for anything.
        if ((metadata.getPersistenceUnit() == null && !Constants.RDBMS_CLIENT_FACTORY.equalsIgnoreCase(client))
                || metadata.getPersistenceUnit() != null && !metadata.getPersistenceUnit().equals(persistenceUnit))
        {
            metadata = null;
        }
        return metadata;
    }
}
