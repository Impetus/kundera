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
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Table;
import javax.persistence.metamodel.Metamodel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.classreading.ClasspathReader;
import com.impetus.kundera.classreading.Reader;
import com.impetus.kundera.classreading.ResourceIterator;
import com.impetus.kundera.loader.MetamodelLoaderException;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.MetadataBuilder;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.IdDiscriptor;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.metadata.processor.GeneratedValueProcessor;
import com.impetus.kundera.metadata.validator.EntityValidator;
import com.impetus.kundera.metadata.validator.EntityValidatorImpl;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * The Metamodel configurer: a) Configure application meta data b) loads entity
 * metadata and maps metadata.
 * 
 * @author vivek.mishra
 */
public class MetamodelConfiguration extends AbstractSchemaConfiguration implements Configuration
{

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(MetamodelConfiguration.class);

    /**
     * Constructor using persistence units as parameter.
     * 
     * @param persistenceUnits
     *            persistence units.
     */
    public MetamodelConfiguration(Map properties, String... persistenceUnits)
    {
        super(persistenceUnits, properties);
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
            Map<String, Object> externalProperties = KunderaCoreUtils.getExternalProperties(persistenceUnit,
                    externalPropertyMap, persistenceUnits);

            client = externalProperties != null ? (String) externalProperties
                    .get(PersistenceProperties.KUNDERA_CLIENT_FACTORY) : null;

            if (client == null)
            {
                client = puMetadata.getClient();
            }
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

        InputStream[] iStreams = null;
        if (this.getClass().getClassLoader() instanceof URLClassLoader)
        {
            URL[] managedClasses = reader.findResources();
            if (managedClasses != null)
            {
                List<URL> managedResources = Arrays.asList(managedClasses);
                managedURLs.addAll(managedResources);
            }
        }
        else
        {
            iStreams = reader.findResourcesAsStream();
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

        Map<String, EntityMetadata> entityMetadataMap = ((MetamodelImpl) metamodel).getEntityMetadataMap();
        Map<String, Class<?>> entityNameToClassMap = ((MetamodelImpl) metamodel).getEntityNameToClassMap();
        Map<String, List<String>> puToClazzMap = new HashMap<String, List<String>>();
        Map<String, IdDiscriptor> entityNameToKeyDiscriptorMap = new HashMap<String, IdDiscriptor>();
        List<Class<?>> classes = new ArrayList<Class<?>>();
        if (resources != null && resources.length > 0)
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
                                persistenceUnit, client, puToClazzMap, entityNameToKeyDiscriptorMap));
                    }
                }
                catch (IOException e)
                {
                    log.error("Error while retreiving and storing entity metadata. Details:", e);
                    throw new MetamodelLoaderException("Error while retreiving and storing entity metadata");

                }
            }
        }
        else if (iStreams != null)
        {
            try
            {
                for (InputStream is : iStreams)
                {
                    try
                    {
                        classes.addAll(scanClassAndPutMetadata(is, reader, entityMetadataMap, entityNameToClassMap,
                                persistenceUnit, client, puToClazzMap, entityNameToKeyDiscriptorMap));
                    }
                    finally
                    {
                        if (is != null)
                        {
                            is.close();
                        }
                    }
                }
            }
            catch (IOException e)
            {
                log.error("Error while retreiving and storing entity metadata. Details:", e);
                throw new MetamodelLoaderException("Error while retreiving and storing entity metadata, Caused by : .",
                        e);

            }
        }
        ((MetamodelImpl) metamodel).setEntityMetadataMap(entityMetadataMap);
        appMetadata.getMetamodelMap().put(persistenceUnit, metamodel);
        appMetadata.setClazzToPuMap(puToClazzMap);
        ((MetamodelImpl) metamodel).addKeyValues(entityNameToKeyDiscriptorMap);
        // assign JPA metamodel.
        ((MetamodelImpl) metamodel).assignEmbeddables(KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getMetaModelBuilder(persistenceUnit).getEmbeddables());
        ((MetamodelImpl) metamodel).assignManagedTypes(KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getMetaModelBuilder(persistenceUnit).getManagedTypes());
        ((MetamodelImpl) metamodel).assignMappedSuperClass(KunderaMetadata.INSTANCE.getApplicationMetadata()
                .getMetaModelBuilder(persistenceUnit).getMappedSuperClassTypes());

        // validateEntityForClientSpecificProperty(classes, persistenceUnit);

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
            String pu = getPersistenceUnitOfEntity(clazz);
            EntityValidator validator = new EntityValidatorImpl(KunderaCoreUtils.getExternalProperties(persistenceUnit,
                    externalPropertyMap, persistenceUnits));
            if (clazz.isAnnotationPresent(Entity.class) && clazz.isAnnotationPresent(Table.class)
                    && persistenceUnit.equalsIgnoreCase(pu))
            {
                validator.validateEntity(clazz);
            }
        }
    }

    private String getPersistenceUnitOfEntity(Class clazz)
    {
        String schema = ((Table) clazz.getAnnotation(Table.class)).schema();
        String pu = null;
        if (schema != null && schema.indexOf("@") > 0)
        {
            pu = schema.substring(schema.indexOf("@") + 1, schema.length());
        }
        return pu;
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
     * @param keyDiscriptor
     * @param persistence
     *            unit the persistence unit.
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private List<Class<?>> scanClassAndPutMetadata(InputStream bits, Reader reader,
            Map<String, EntityMetadata> entityMetadataMap, Map<String, Class<?>> entityNameToClassMap,
            String persistenceUnit, String client, Map<String, List<String>> clazzToPuMap,
            Map<String, IdDiscriptor> entityNameToKeyDiscriptorMap) throws IOException
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
                    // Class<?> clazz =
                    // Thread.currentThread().getContextClassLoader().loadClass(className);

                    Class<?> clazz = this.getClass().getClassLoader().loadClass(className);

                    if (entityNameToClassMap.containsKey(clazz.getSimpleName())
                            && !entityNameToClassMap.get(clazz.getSimpleName()).getName().equals(clazz.getName()))
                    {
                        throw new MetamodelLoaderException("Name conflict between classes "
                                + entityNameToClassMap.get(clazz.getSimpleName()).getName() + " and " + clazz.getName()
                                + ". Make sure no two entity classes with the same name "
                                + " are specified for persistence unit " + persistenceUnit);
                    }

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
                                MetadataBuilder metadataBuilder = new MetadataBuilder(persistenceUnit, client,
                                        KunderaCoreUtils.getExternalProperties(persistenceUnit, externalPropertyMap,
                                                persistenceUnits));
                                metadata = metadataBuilder.buildEntityMetadata(clazz);

                                // in case entity's pu does not belong to parse
                                // persistence unit, it will be null.
                                if (metadata != null)
                                {
                                    entityMetadataMap.put(clazz.getName(), metadata);
                                    mapClazztoPu(clazz, persistenceUnit, clazzToPuMap);
                                    processGeneratedValueAnnotation(clazz, persistenceUnit, metadata,
                                            entityNameToKeyDiscriptorMap);
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
            if (dstream != null)
            {
                dstream.close();
            }
            if (bits != null)
            {
                bits.close();
            }
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

    private void processGeneratedValueAnnotation(Class<?> clazz, String persistenceUnit, EntityMetadata m,
            Map<String, IdDiscriptor> entityNameToKeyDiscriptorMap)
    {
        GeneratedValueProcessor processer = new GeneratedValueProcessor();
        String pu = getPersistenceUnitOfEntity(clazz);
        String clientFactoryName = KunderaMetadataManager.getPersistenceUnitMetadata(m.getPersistenceUnit())
                .getClient();
        if (pu != null && pu.equals(persistenceUnit)
                || clientFactoryName.equalsIgnoreCase("com.impetus.client.rdbms.RDBMSClientFactory"))
        {
            Field f = (Field) m.getIdAttribute().getJavaMember();

            if (f.isAnnotationPresent(GeneratedValue.class))
            {
                processer.process(clazz, f, m, entityNameToKeyDiscriptorMap);
            }
        }
    }
}