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
package com.impetus.kundera.configure;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.metamodel.Metamodel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.classreading.ClasspathReader;
import com.impetus.kundera.classreading.Reader;
import com.impetus.kundera.classreading.ResourceIterator;
import com.impetus.kundera.loader.MetamodelLoaderException;
import com.impetus.kundera.metadata.MetadataBuilder;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.validator.EntityValidator;
import com.impetus.kundera.metadata.validator.EntityValidatorImpl;

/**
 * The Metamodel configurer: a) Configure application meta data b) loads entity
 * metadata and maps metadata.
 * 
 * @author vivek.mishra
 */
public class MetamodelConfiguration implements Configuration
{

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(MetamodelConfiguration.class);

    /** Holding persistence unit instances. */
    private String[] persistenceUnits;

    /**
     * Constructor using persistence units as parameter.
     * 
     * @param persistenceUnits
     *            persistence units.
     */
    public MetamodelConfiguration(String... persistenceUnits)
    {
        this.persistenceUnits = persistenceUnits;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.configure.Configuration#configure()
     */
    @Override
    public void configure()
    {
        log.debug("Loading Entity Metadata...");
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();

        for (String persistenceUnit : persistenceUnits)
        {
            if (appMetadata.getMetamodelMap().get(persistenceUnit.trim()) != null)
            {
                log.debug("Metadata already exists for the Persistence Unit " + persistenceUnit + ". Nothing to do");
            }
            else
            {
                loadEntityMetadata(persistenceUnit);
            }
        }

    }

    /**
     * Load entity metadata.
     * 
     * @param persistenceUnit
     *            the persistence unit
     */
    private void loadEntityMetadata(String persistenceUnit)
    {
        if (persistenceUnit == null)
        {
            throw new IllegalArgumentException(
                    "Must have a persistenceUnitName in order to load entity metadata, you provided:" + persistenceUnit);
        }

        KunderaMetadata kunderaMetadata = KunderaMetadata.INSTANCE;
        Map<String, PersistenceUnitMetadata> persistentUnitMetadataMap = kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadataMap();

        /** Classes to scan */
        List<String> classesToScan;
        URL[] resources = null;
        String client = null;
        List<URL> managedURLs = null;
        if (persistentUnitMetadataMap == null || persistentUnitMetadataMap.isEmpty())
        {
            log.error("It is necessary to load Persistence Unit metadata  for persistence unit " + persistenceUnit
                    + " first before loading entity metadata.");
            throw new MetamodelLoaderException("load Persistence Unit metadata  for persistence unit "
                    + persistenceUnit + " first before loading entity metadata.");
        }
        else
        {
            PersistenceUnitMetadata puMetadata = persistentUnitMetadataMap.get(persistenceUnit);
            classesToScan = puMetadata.getManagedClassNames();
            managedURLs = puMetadata.getManagedURLs();
            client = puMetadata.getClient();
        }

        /*
         * Check whether Classes to scan was provided into persistence.xml If
         * yes, load them. Otherwise load them from classpath/ context path
         */
        Reader reader;
        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();
        if (classesToScan == null || classesToScan.isEmpty())
        {
            log.info("No class to scan for persistence unit " + persistenceUnit
                    + ". Entities will be loaded from classpath/ context-path");
            // Entity metadata is not related to any PU, and hence will be
            // stored at common place
            // persistenceUnit = Constants.COMMON_ENTITY_METADATAS;

            // Check whether all common entity metadata have already been loaded
            if (appMetadata.getMetamodelMap().get(persistenceUnit) != null)
            {
                log.info("All common entitity metadata already loaded, nothing need to be done");
                return;
            }

            reader = new ClasspathReader();
            // resources = reader.findResourcesByClasspath();
        }
        else
        {
            reader = new ClasspathReader(classesToScan);
            // resources = reader.findResourcesByContextLoader();
        }

        URL[] managedClasses = reader.findResources();
        if (managedClasses != null)
        {
            List<URL> managedResources = Arrays.asList(managedClasses);
            managedURLs.addAll(managedResources);
        }

        if (managedURLs != null)
        {
            resources = managedURLs.toArray(new URL[] {});
        }

        // All entities to load should be annotated with @Entity
        reader.addValidAnnotations(Entity.class.getName());

        Metamodel metamodel = appMetadata.getMetamodel(persistenceUnit);
        if (metamodel == null)
        {
            metamodel = new MetamodelImpl();
        }

        Map<Class<?>, EntityMetadata> entityMetadataMap = ((MetamodelImpl) metamodel).getEntityMetadataMap();
        Map<String, Class<?>> entityNameToClassMap = ((MetamodelImpl) metamodel).getEntityNameToClassMap();
        Map<String, List<String>> puToClazzMap = new HashMap<String, List<String>>();
        List<Class<?>> classes = new ArrayList<Class<?>>();

        if (resources != null)
        {
            for (URL resource : resources)
            {
                try
                {
                    ResourceIterator itr = reader.getResourceIterator(resource, reader.getFilter());

                    InputStream is = null;
                    while ((is = itr.next()) != null)
                    {
                        classes.addAll(scanClassAndPutMetadata(is, reader, entityMetadataMap, entityNameToClassMap,
                                persistenceUnit, client, puToClazzMap));
                    }
                }
                catch (IOException e)
                {
                    log.error("Error while retreiving and storing entity metadata. Details:" + e.getMessage());
                    throw new MetamodelLoaderException("Error while retreiving and storing entity metadata");

                }
            }
        }
        ((MetamodelImpl) metamodel).setEntityMetadataMap(entityMetadataMap);
        appMetadata.getMetamodelMap().put(persistenceUnit, metamodel);
        appMetadata.setClazzToPuMap(puToClazzMap);

        // assign JPA metamodel.
        ((MetamodelImpl) metamodel).assignEmbeddables(KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getMetaModelBuilder(persistenceUnit).getEmbeddables());
        ((MetamodelImpl) metamodel).assignManagedTypes(KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getMetaModelBuilder(persistenceUnit).getManagedTypes());
        ((MetamodelImpl) metamodel).assignMappedSuperClass(KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getMetaModelBuilder(persistenceUnit).getMappedSuperClassTypes());

        validateEntityForClientSpecificProperty(classes, persistenceUnit);

    }

    /**
     * @param resources
     * @param reader
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private void validateEntityForClientSpecificProperty(List<Class<?>> classes, final String persistenceUnit)
    {
        for (Class clazz : classes)
        {
            String schema = ((Table) clazz.getAnnotation(Table.class)).schema();
            String pu = null;
            if (schema != null && schema.indexOf("@") > 0)
            {
                pu = schema.substring(schema.indexOf("@") + 1, schema.length());
            }
            EntityValidator validator = new EntityValidatorImpl();
            if (clazz.isAnnotationPresent(Entity.class) && clazz.isAnnotationPresent(Table.class)
                    && persistenceUnit.equalsIgnoreCase(pu))
            {
                validator.validateEntity(clazz);
            }
        }
    }

    /**
     * Scan class and put metadata.
     * 
     * @param bits
     *            the bits
     * @param reader
     *            the reader
     * @param entityMetadataMap
     *            the entity metadata map
     * @param entityNameToClassMap
     *            the entity name to class map
     * @param persistence
     *            unit the persistence unit.
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private List<Class<?>> scanClassAndPutMetadata(InputStream bits, Reader reader,
            Map<Class<?>, EntityMetadata> entityMetadataMap, Map<String, Class<?>> entityNameToClassMap,
            String persistenceUnit, String client, Map<String, List<String>> clazzToPuMap) throws IOException
    {
        DataInputStream dstream = new DataInputStream(new BufferedInputStream(bits));
        ClassFile cf = null;
        String className = null;

        List<Class<?>> classes = new ArrayList<Class<?>>();

        try
        {
            cf = new ClassFile(dstream);

            className = cf.getName();
            List<String> annotations = new ArrayList<String>();

            reader.accumulateAnnotations(annotations,
                    (AnnotationsAttribute) cf.getAttribute(AnnotationsAttribute.visibleTag));
            reader.accumulateAnnotations(annotations,
                    (AnnotationsAttribute) cf.getAttribute(AnnotationsAttribute.invisibleTag));

            // iterate through all valid annotations
            for (String validAnn : reader.getValidAnnotations())
            {
                // check if the current class has one?
                if (annotations.contains(validAnn))
                {
                    Class<?> clazz = Class.forName(className);

                    if (entityNameToClassMap.containsKey(clazz.getSimpleName())
                            && !entityNameToClassMap.get(clazz.getSimpleName()).getName().equals(clazz.getName()))
                    {
                        throw new MetamodelLoaderException("Name conflict between classes "
                                + entityNameToClassMap.get(clazz.getSimpleName()).getName() + " and " + clazz.getName()
                                + ". Make sure no two entity classes with the same name "
                                + " are specified for persistence unit " + persistenceUnit);
                    }

                    // This is required just to keep hibernate happy.
                    // As somehow it complains for lazily loading of entities
                    // while building session factory.

                    entityNameToClassMap.put(clazz.getSimpleName(), clazz);

                    EntityMetadata metadata = entityMetadataMap.get(clazz);
                    if (null == metadata)
                    {
                        log.debug("Metadata not found in cache for " + clazz.getName());
                        // double check locking.
                        synchronized (clazz)
                        {
                            if (null == metadata)
                            {
                                MetadataBuilder metadataBuilder = new MetadataBuilder(persistenceUnit, client);
                                metadata = metadataBuilder.buildEntityMetadata(clazz);

                                // in case entity's pu does not belong to parse
                                // persistence unit, it will be null.
                                if (metadata != null)
                                {
                                    entityMetadataMap.put(clazz, metadata);
                                    mapClazztoPu(clazz, persistenceUnit, clazzToPuMap);
                                }
                            }
                        }
                    }

                    // TODO :
                    onValidateClientProperties(classes, clazz, persistenceUnit);

                }
            }
        }
        catch (ClassNotFoundException e)
        {
            log.error("Class " + className + " not found, it won't be loaded as entity");
        }
        finally
        {
            dstream.close();
            bits.close();
        }

        return classes;
    }

    /**
     * @param clazz
     */
    private List<Class<?>> onValidateClientProperties(List<Class<?>> classes, Class<?> clazz,
            final String persistenceUnit)
    {
        if (clazz.isAnnotationPresent(Entity.class) && clazz.isAnnotationPresent(Table.class))
        {
            classes.add(clazz);
        }
        return classes;
    }

    /**
     * Method to prepare class simple name to list of pu's mapping. 1 class can
     * be mapped to multiple persistence units, in case of RDBMS, in other cases
     * it will only be 1!
     * 
     * @param clazz
     *            entity class to be mapped.
     * @param pu
     *            current persistence unit name
     * @param clazzToPuMap
     *            collection holding mapping.
     * @return map holding mapping.
     */
    private Map<String, List<String>> mapClazztoPu(Class<?> clazz, String pu, Map<String, List<String>> clazzToPuMap)
    {
        List<String> puCol = new ArrayList<String>(1);
        if (clazzToPuMap == null)
        {
            clazzToPuMap = new HashMap<String, List<String>>();
        }
        else
        {
            if (clazzToPuMap.containsKey(clazz.getName()))
            {
                puCol = clazzToPuMap.get(clazz.getName());
            }
        }

        if (!puCol.contains(pu))
        {
            puCol.add(pu);
            clazzToPuMap.put(clazz.getName(), puCol);
        }

        return clazzToPuMap;
    }

}