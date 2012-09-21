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
package com.impetus.kundera.metadata.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.metamodel.Metamodel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.configure.schema.SchemaMetadata;
import com.impetus.kundera.metadata.processor.MetaModelBuilder;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * Application metadata refers to metdata specific to application(e.g. metamodel
 * collection, persistence unit metdatas) Any reference which is out of
 * persistence unit metadata and entity specific metadata is held by this class.
 * 
 * @author amresh.singh
 */
public class ApplicationMetadata
{
    /** Map of Entity Metadata. */
    private Map<String, Metamodel> metamodelMap = new ConcurrentHashMap<String, Metamodel>();

    /** Map of Persistence Unit Metadata. */
    private Map<String, PersistenceUnitMetadata> persistenceUnitMetadataMap = new ConcurrentHashMap<String, PersistenceUnitMetadata>();

    /** The Constant log. */
    private static Log logger = LogFactory.getLog(EntityManagerFactoryImpl.class);

    private SchemaMetadata schemaMetadata = new SchemaMetadata();

    // private MetaModelBuilder metaModelBuilder = new MetaModelBuilder();

    private Map<String, MetaModelBuilder> metaModelBuilder = new ConcurrentHashMap<String, MetaModelBuilder>();

    /**
     * Collection instance to hold clazz's full name to persistence unit
     * mapping. Valid Assumption: 1 class can belong to 1 pu only. Reason is @table
     * needs to give pu name!
     */
    private Map<String, List<String>> clazzToPuMap;

    private Map<String, QueryWrapper> namedNativeQueries;

    /**
     * Adds the entity metadata.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param clazz
     *            the clazz
     * @param entityMetadata
     *            the entity metadata
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
     * Adds the persistence unit metadata.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @param persistenceUnitMetadata
     *            the persistence unit metadata
     */
    public void addPersistenceUnitMetadata(Map<String, PersistenceUnitMetadata> metadata)
    {
        getPersistenceUnitMetadataMap().putAll(metadata);
    }

    /**
     * Gets the metamodel map.
     * 
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

    /**
     * Gets the persistence unit metadata.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @return the persistence unit metadata
     */
    public PersistenceUnitMetadata getPersistenceUnitMetadata(String persistenceUnit)
    {
        return getPersistenceUnitMetadataMap().get(persistenceUnit);
    }

    /**
     * Gets the metamodel.
     * 
     * @param persistenceUnit
     *            the persistence unit
     * @return the metamodel
     */
    public Metamodel getMetamodel(String persistenceUnit)
    {
        Map<String, Metamodel> model = getMetamodelMap();
        return persistenceUnit != null && model.containsKey(persistenceUnit) ? model.get(persistenceUnit) : null;
    }

    /**
     * Gets the persistence unit metadata map.
     * 
     * @return the persistenceUnitMetadataMap
     */
    public Map<String, PersistenceUnitMetadata> getPersistenceUnitMetadataMap()
    {
        return persistenceUnitMetadataMap;
    }

    /**
     * Sets the clazz to pu map.
     * 
     * @param map
     *            the map
     */
    public void setClazzToPuMap(Map<String, List<String>> map)
    {
        if (clazzToPuMap == null)
        {
            this.clazzToPuMap = map;
        }
        else
        {
            clazzToPuMap.putAll(map);
        }
    }

    /**
     * Gets the mapped persistence unit.
     * 
     * @param clazz
     *            the clazz
     * 
     * @return the mapped persistence unit
     */
    public List<String> getMappedPersistenceUnit(Class<?> clazz)
    {
        return this.clazzToPuMap != null ? this.clazzToPuMap.get(clazz.getName()) : null;
    }

    /**
     * returns mapped persistence unit.
     * 
     * @param clazzName
     *            clazz name.
     * 
     * @return mapped persistence unit.
     */
    public String getMappedPersistenceUnit(String clazzName)
    {

        List<String> pus = clazzToPuMap.get(clazzName);

        final int _first = 0;
        String pu = null;

        if (pus != null && !pus.isEmpty())
        {
            if (pus.size() == 2)
            {
                onError(clazzName);
            }
            return pus.get(_first);
        }
        else
        {
            Set<String> mappedClasses = this.clazzToPuMap.keySet();
            boolean found = false;
            for (String clazz : mappedClasses)
            {
                if (found && clazz.endsWith("." + clazzName))
                {
                    onError(clazzName);
                }
                else if (clazz.endsWith("." + clazzName))
                {
                    pu = clazzToPuMap.get(clazz).get(_first);
                    found = true;
                }
            }
        }

        return pu;
    }

    /**
     * Adds parameterised query with given name into collection. Throws
     * exception if duplicate name is provided.
     * 
     * @param queryName
     *            query name.
     * @param query
     *            named/native query.
     * @param isNativeQuery
     *            true, if it is a namednativequery.
     * 
     */
    public void addQueryToCollection(String queryName, String query, boolean isNativeQuery, Class clazz)
    {
        if (namedNativeQueries == null)
        {
            namedNativeQueries = new HashMap<String, QueryWrapper>();
        }
        if (!namedNativeQueries.containsKey(queryName))
        {
            namedNativeQueries.put(queryName, new QueryWrapper(queryName, query, isNativeQuery, clazz));
        }
        // No null check made as it will never hold null value
        else if (!getQuery(queryName).equals(query))
        {
            logger.error("Duplicate named/native query with name:" + queryName
                    + "found! Already there is a query with same name:" + namedNativeQueries.get(queryName));
            throw new ApplicationLoaderException("Duplicate named/native query with name:" + queryName
                    + "found! Already there is a query with same name:" + namedNativeQueries.get(queryName));
        }
    }

    /**
     * Returns query interface.
     * 
     * @param name
     *            query name.
     * @return query.
     */
    public String getQuery(String name)
    {
        QueryWrapper wrapper = namedNativeQueries != null ? namedNativeQueries.get(name) : null;
        return wrapper != null ? wrapper.getQuery() : null;
    }

    /**
     * Returns true, if query is named native or native, else false
     * 
     * @param name
     *            mapped name.
     * @return boolean value
     */
    public boolean isNative(String name)
    {
        QueryWrapper wrapper = namedNativeQueries != null ? namedNativeQueries.get(name) : null;
        return wrapper != null ? wrapper.isNativeQuery() : false;
    }

    public Class getMappedClass(String name)
    {
        QueryWrapper wrapper = namedNativeQueries != null ? namedNativeQueries.get(name) : null;
        return wrapper != null ? wrapper.getMappedClazz() : null;
    }

    /**
     * Handler error and log statements.
     * 
     * @param clazzName
     *            class name.
     */
    private void onError(String clazzName)
    {
        logger.error("Duplicate name:" + clazzName + "Please provide entity with complete package name.");
        throw new ApplicationLoaderException("Duplicate name:" + clazzName
                + "Please provide entity with complete package name");
    }

    private class QueryWrapper
    {
        String queryName;

        String query;

        boolean isNativeQuery;

        Class entityClazz;

        /**
         * @param queryName
         * @param query
         * @param isNativeQuery
         */
        public QueryWrapper(String queryName, String query, boolean isNativeQuery, Class clazz)
        {
            this.queryName = queryName;
            this.query = query;
            this.isNativeQuery = isNativeQuery;
            this.entityClazz = clazz;
        }

        /**
         * @return the query
         */
        String getQuery()
        {
            return query;
        }

        /**
         * @return the isNativeQuery
         */
        boolean isNativeQuery()
        {
            return isNativeQuery;
        }

        Class getMappedClazz()
        {
            return entityClazz;
        }
    }

    /**
     * @return the schemaMetadata
     */
    public SchemaMetadata getSchemaMetadata()
    {
        return schemaMetadata;
    }

    /**
     * @return the metaModelBuilder
     */
    public MetaModelBuilder getMetaModelBuilder(String persistenceUnit)
    {

        if (metaModelBuilder.containsKey(persistenceUnit))
        {
            return metaModelBuilder.get(persistenceUnit);
        }
        else
        {
            MetaModelBuilder builder = new MetaModelBuilder();
            metaModelBuilder.put(persistenceUnit, builder);
            return builder;
        }
    }

}
