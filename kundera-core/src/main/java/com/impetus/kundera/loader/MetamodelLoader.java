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
package com.impetus.kundera.loader;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;

import javax.persistence.Entity;
import javax.persistence.PersistenceException;
import javax.persistence.metamodel.Metamodel;

import org.apache.log4j.Logger;

import com.impetus.kundera.classreading.ClasspathReader;
import com.impetus.kundera.classreading.Reader;
import com.impetus.kundera.classreading.ResourceIterator;
import com.impetus.kundera.metadata.MetadataBuilder;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * @author amresh.singh
 * 
 */
public class MetamodelLoader extends ApplicationLoader
{
    private static Logger log = Logger.getLogger(MetamodelLoader.class);

    @Override
    public void load(String persistenceUnit)
    {
        log.debug("Loading Entity Metadata...");
        KunderaMetadata kunderaMetadata = KunderaMetadata.getInstance();
        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();
        if (appMetadata.getMetamodelMap().get(persistenceUnit) != null)
        {
            log.debug("Metadata already exists for the Persistence Unit " + persistenceUnit + ". Nothing to do");
        }
        else
        {
            loadEntityMetadata(persistenceUnit);
        }
    }

    private void loadEntityMetadata(String persistenceUnit)
    {
        if (persistenceUnit == null)
        {
            throw new IllegalArgumentException("Must have a persistenceUnitName!");
        }

        KunderaMetadata kunderaMetadata = KunderaMetadata.getInstance();
        Map<String, PersistenceUnitMetadata> persistentUnitMetadataMap = kunderaMetadata.getApplicationMetadata()
                .getPersistenceUnitMetadataMap();

        /** Classes to scan */
        List<String> classesToScan;

        if (persistentUnitMetadataMap == null || persistentUnitMetadataMap.isEmpty())
        {
            log.error("It is necessary to load Persistence Unit metadata  for persistence unit " + persistenceUnit
                    + " first before loading entity metadata.");
            throw new PersistenceException("load Persistence Unit metadata  for persistence unit " + persistenceUnit
                    + " first before loading entity metadata.");
        }
        else
        {
            PersistenceUnitMetadata puMetadata = persistentUnitMetadataMap.get(persistenceUnit);
            classesToScan = puMetadata.getClasses();

        }

        /*
         * Check whether Classes to scan was provided into persistence.xml If
         * yes, load them. Otherwise load them from classpath/ context path
         */
        Reader reader;
        URL[] resources;
        if (classesToScan == null || classesToScan.isEmpty())
        {
            log.info("No class to scan for persistence unit " + persistenceUnit
                    + ". Entities will be loaded from classpath/ context-path");
            reader = new ClasspathReader();
            resources = reader.findResourcesByClasspath();
        }
        else
        {
            reader = new ClasspathReader(classesToScan);
            resources = reader.findResourcesByContextLoader();
        }
        // All entities to load should be annotated with @Entity
        reader.addValidAnnotations(Entity.class.getName());

        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();
        Metamodel metamodel = appMetadata.getMetamodel(persistenceUnit);
        if (metamodel == null)
        {
            metamodel = new MetamodelImpl();
        }

        Map<Class<?>, EntityMetadata> entityMetadataMap = ((MetamodelImpl) metamodel).getEntityMetadataMap();
        Map<String, Class<?>> entityNameToClassMap = ((MetamodelImpl) metamodel).getEntityNameToClassMap();

        for (URL resource : resources)
        {
            try
            {
                ResourceIterator itr = reader.getResourceIterator(resource, reader.getFilter());

                InputStream is = null;
                while ((is = itr.next()) != null)
                {
                    scanClassAndPutMetadata(is, reader, entityMetadataMap, entityNameToClassMap);
                }
            }
            catch (IOException e)
            {
                // TODO: Do something with this exception
                log.error("Error while retreiving and storing entity metadata. Details:" + e.getMessage());
                e.printStackTrace();
            }
        }
        ((MetamodelImpl) metamodel).setEntityMetadataMap(entityMetadataMap);
        appMetadata.getMetamodelMap().put(persistenceUnit, metamodel);
    }

    private void scanClassAndPutMetadata(InputStream bits, Reader reader,
            Map<Class<?>, EntityMetadata> entityMetadataMap, Map<String, Class<?>> entityNameToClassMap)
            throws IOException
    {
        DataInputStream dstream = new DataInputStream(new BufferedInputStream(bits));
        ClassFile cf = null;
        String className = null;

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

                    if (entityNameToClassMap.containsKey(clazz.getSimpleName()))
                    {
                        throw new PersistenceException("Name conflict between classes "
                                + entityNameToClassMap.get(clazz.getSimpleName()).getName() + " and " + clazz.getName());
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
                                MetadataBuilder metadataBuilder = new MetadataBuilder();
                                metadata = metadataBuilder.buildEntityMetadata(clazz);
                                entityMetadataMap.put(clazz, metadata);
                            }
                        }
                    }

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
    }

}
