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
package com.impetus.kundera.metadata.model;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.metamodel.Metamodel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * @author amresh.singh
 * 
 */
public class ApplicationMetadata
{
    /** Map of Entity Metadata. */
    private Map<String, Metamodel> metamodelMap = new ConcurrentHashMap<String, Metamodel>();

    /** Map of Persistence Unit Metadata. */
    private Map<String, PersistenceUnitMetadata> persistenceUnitMetadataMap = new ConcurrentHashMap<String, PersistenceUnitMetadata>();

    /** The Constant log. */
    private static Log logger = LogFactory.getLog(EntityManagerFactoryImpl.class);

    /**
     * @param metamodelMap
     *            the entityMetadataMap to set
     */
    public void addEntityMetadata(String persistenceUnit, Class<?> clazz, EntityMetadata entityMetadata)
    {
        Metamodel metamodel = getMetamodelMap().get(persistenceUnit);
        Map<Class<?>, EntityMetadata> entityClassToMetadataMap = ((MetamodelImpl) metamodel).getEntityMetadataMap();
        if (entityClassToMetadataMap == null || entityClassToMetadataMap.isEmpty())
        {
            entityClassToMetadataMap.put(clazz, entityMetadata);
        }
        else
        {
            logger.debug("Entity meta model already exists for persistence unit " + persistenceUnit + " and class "
                    + clazz + ". Noting needs to be done");
        }
    }

    /**
     * @param persistenceUnitMetadataMap
     *            the persistenceUnitMetadataMap to set
     */
    public void addPersistenceUnitMetadata(String persistenceUnit, PersistenceUnitMetadata persistenceUnitMetadata)
    {
        getPersistenceUnitMetadataMap().put(persistenceUnit, persistenceUnitMetadata);
    }

    /**
     * @return the entityMetadataMap
     */
    public Map<String, Metamodel> getMetamodelMap()
    {
        if (metamodelMap == null)
        {
            metamodelMap = new HashMap<String, Metamodel>();
        }
        return metamodelMap;
    }

    public PersistenceUnitMetadata getPersistenceUnitMetadata(String persistenceUnit)
    {
        return getPersistenceUnitMetadataMap().get(persistenceUnit);
    }

    public Metamodel getMetamodel(String persistenceUnit)
    {
        return getMetamodelMap().get(persistenceUnit);
    }

    /**
     * @return the persistenceUnitMetadataMap
     */
    public Map<String, PersistenceUnitMetadata> getPersistenceUnitMetadataMap()
    {
        return persistenceUnitMetadataMap;
    }
}
