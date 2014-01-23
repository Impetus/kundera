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
import java.util.Map;

import javax.persistence.NamedNativeQueries;
import javax.persistence.NamedNativeQuery;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.PersistenceException;
import javax.persistence.Table;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.processor.CacheableAnnotationProcessor;
import com.impetus.kundera.metadata.processor.EntityListenersProcessor;
import com.impetus.kundera.metadata.processor.IndexProcessor;
import com.impetus.kundera.metadata.processor.TableProcessor;
import com.impetus.kundera.metadata.validator.EntityValidator;
import com.impetus.kundera.metadata.validator.EntityValidatorImpl;
import com.impetus.kundera.metadata.validator.InvalidEntityDefinitionException;

/**
 * Concrete implementation of IMetadataManager.
 * 
 * @author animesh.kumar
 */
public class MetadataBuilder
{

    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(MetadataBuilder.class);

    /** The metadata processors. */
    private List<MetadataProcessor> metadataProcessors;

    /** The Validator. */
    private EntityValidator validator;

    /** persistence unit */
    private String persistenceUnit;

    /** kundera client */
    private String client;

    private Map puProperties;

    /**
     * Instantiates a new metadata manager.
     * 
     */

    public MetadataBuilder(String puName, String client, Map puProperties)
    {
        this.persistenceUnit = puName;
        this.client = client;
        this.puProperties = puProperties;
        this.validator = new EntityValidatorImpl(puProperties);
        this.metadataProcessors = new ArrayList<MetadataProcessor>();

        // add processors to chain.
        this.metadataProcessors.add(new TableProcessor(puProperties));
        this.metadataProcessors.add(new CacheableAnnotationProcessor());
        this.metadataProcessors.add(new IndexProcessor());
        this.metadataProcessors.add(new EntityListenersProcessor());
        
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
     * @param externalProperties
     * @return the entity metadata
     */
    public EntityMetadata buildEntityMetadata(Class<?> clazz)
    {

        EntityMetadata metadata = new EntityMetadata(clazz);
        validate(clazz);

        if (log.isDebugEnabled())
            log.debug("Processing @Entity >> " + clazz);

        for (MetadataProcessor processor : metadataProcessors)
        {
            // // in case it is not intend for current persistence unit.
            // checkForRDBMS(metadata);
            // checkForNeo4J(metadata);

            setSchemaAndPU(clazz, metadata);

            processor.process(clazz, metadata);
            metadata = belongsToPersistenceUnit(metadata);
            if (metadata == null)
            {
                break;
            }

            // Check for schema attribute of Table annotation.
            if (MetadataUtils.isSchemaAttributeRequired(metadata.getPersistenceUnit())
                    && StringUtils.isBlank(metadata.getSchema()))
            {
                if (log.isErrorEnabled())
                {
                    log.error("It is mandatory to specify Schema alongwith Table name:" + metadata.getTableName()
                            + ". This entity won't be persisted");
                }
                throw new InvalidEntityDefinitionException("It is mandatory to specify Schema alongwith Table name:"
                        + metadata.getTableName() + ". This entity won't be persisted");
            }
        }

        return metadata;
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

        PersistenceUnitMetadata puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        String keyspace = puProperties != null ? (String) puProperties.get(PersistenceProperties.KUNDERA_KEYSPACE):null;
        
        keyspace = keyspace == null ? puMetadata.getProperty(PersistenceProperties.KUNDERA_KEYSPACE):keyspace;

        if (metadata.getPersistenceUnit() != null && !metadata.getPersistenceUnit().equals(persistenceUnit)
                || (keyspace != null && metadata.getSchema() != null && !metadata.getSchema().equals(keyspace)))
        {
            metadata = null;
        }
        else
        {
            applyMetadataChanges(metadata);
        }

        /*
         * if ((metadata.getPersistenceUnit() == null &&
         * !(Constants.RDBMS_CLIENT_FACTORY.equalsIgnoreCase(client) ||
         * Constants.NEO4J_CLIENT_FACTORY .equalsIgnoreCase(client))) ||
         * metadata.getPersistenceUnit() != null &&
         * !metadata.getPersistenceUnit().equals(persistenceUnit)) { metadata =
         * null; }
         */

        return metadata;
    }

    private void applyMetadataChanges(EntityMetadata metadata)
    {
//        log.debug("In apply changes class is " + metadata.getEntityClazz().getName());
//        log.debug("In apply changes pu is " + persistenceUnit);
        metadata.setPersistenceUnit(persistenceUnit);
        PersistenceUnitMetadata puMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getPersistenceUnitMetadata(persistenceUnit);
        
        String keyspace = puProperties != null ? (String) puProperties.get(PersistenceProperties.KUNDERA_KEYSPACE):null;
        
        keyspace = keyspace == null ? puMetadata.getProperty(PersistenceProperties.KUNDERA_KEYSPACE):keyspace;

        // precedence to @Table annotation.
        if (metadata.getSchema() == null)
        {
            metadata.setSchema(keyspace);
        }
        if (metadata.getTableName() == null)
        {
            metadata.setTableName(metadata.getEntityClazz().getSimpleName());
        }
    }

    private void setSchemaAndPU(Class<?> clazz, EntityMetadata metadata)
    {
        Table table = clazz.getAnnotation(Table.class);
        if (table != null)
        {
//            log.debug("In set schema and pu, class is " + clazz.getName());
            // Set Name of persistence object
            metadata.setTableName(!StringUtils.isBlank(table.name()) ? 
                     table.name() : clazz.getSimpleName());
            // Add named/native query related application metadata.
            addNamedNativeQueryMetadata(clazz);
            // set schema name and persistence unit name (if provided)
            String schemaStr = table.schema();

            MetadataUtils.setSchemaAndPersistenceUnit(metadata, schemaStr, puProperties);
        }
        if (metadata.getPersistenceUnit() == null)
        {
//            log.debug("In set schema and pu, pu is " + persistenceUnit);
            metadata.setPersistenceUnit(persistenceUnit);
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

}
