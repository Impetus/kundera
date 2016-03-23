/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Table;

import org.apache.commons.lang.StringUtils;

import com.impetus.kundera.Constants;
import com.impetus.kundera.KunderaException;
import com.impetus.kundera.configure.SchemaConfiguration;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.processor.IndexProcessor;
import com.impetus.kundera.metadata.processor.TableProcessor;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * The Class DefaultKunderaEntity.
 *
 * @param <T>
 *            the generic type
 * @param <K>
 *            the key type
 */
public class DefaultKunderaEntity<T, K> implements KunderaEntity<T, K>
{

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.core.KunderaEntity#find(java.lang.Object)
     */
    public final T find(K key)
    {
        return (T) em.find(this.getClass(), key);
    }

    /**
     * On bind.
     *
     * @param clazz
     *            the clazz
     */
    private static void onBind(Class clazz)
    {

        if (((MetamodelImpl) em.getMetamodel()).getEntityMetadataMap().isEmpty())
        {

            EntityMetadata metadata = new EntityMetadata(clazz);
            metadata.setPersistenceUnit(getPersistenceUnit());

            setSchemaAndPU(clazz, metadata);

            new TableProcessor(em.getEntityManagerFactory().getProperties(),
                    ((EntityManagerFactoryImpl) em.getEntityManagerFactory()).getKunderaMetadataInstance())
                            .process(clazz, metadata);

            KunderaMetadata kunderaMetadata = ((EntityManagerFactoryImpl) em.getEntityManagerFactory())
                    .getKunderaMetadataInstance();

            new IndexProcessor(kunderaMetadata).process(clazz, metadata);

            ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();

            ((MetamodelImpl) em.getMetamodel()).addEntityMetadata(clazz, metadata);
            ((MetamodelImpl) em.getMetamodel()).addEntityNameToClassMapping(clazz.getSimpleName(), clazz);
            appMetadata.getMetamodelMap().put(getPersistenceUnit(), em.getMetamodel());

            Map<String, List<String>> clazzToPuMap = new HashMap<String, List<String>>();
            List<String> persistenceUnits = new ArrayList<String>();
            persistenceUnits.add(getPersistenceUnit());
            clazzToPuMap.put(clazz.getName(), persistenceUnits);
            appMetadata.setClazzToPuMap(clazzToPuMap);
            new SchemaConfiguration(em.getEntityManagerFactory().getProperties(), kunderaMetadata, getPersistenceUnit())
                    .configure();

        }

    }

    /**
     * Sets the schema and pu.
     *
     * @param clazz
     *            the clazz
     * @param metadata
     *            the metadata
     */
    private static void setSchemaAndPU(Class<?> clazz, EntityMetadata metadata)
    {
        Table table = clazz.getAnnotation(Table.class);
        if (table != null)
        {
            metadata.setTableName(!StringUtils.isBlank(table.name()) ? table.name() : clazz.getSimpleName());
            String schemaStr = table.schema();

            MetadataUtils.setSchemaAndPersistenceUnit(metadata, schemaStr,
                    em.getEntityManagerFactory().getProperties());
        }
        else
        {
            metadata.setTableName(clazz.getSimpleName());
            metadata.setSchema((String) em.getEntityManagerFactory().getProperties().get("kundera.keyspace"));
        }

        if (metadata.getPersistenceUnit() == null)
        {
            metadata.setPersistenceUnit(getPersistenceUnit());
        }
    }

    /**
     * Gets the persistence unit.
     *
     * @return the persistence unit
     */
    private static String getPersistenceUnit()
    {
        return (String) em.getEntityManagerFactory().getProperties().get(Constants.PERSISTENCE_UNIT_NAME);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.core.KunderaEntity#save()
     */
    public final void save()
    {
        em.persist(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.core.KunderaEntity#update()
     */
    public final void update()
    {
        em.merge(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.core.KunderaEntity#delete()
     */
    public final void delete()
    {
        em.remove(this);
    }

    /**
     * Bind.
     *
     * @param propertiesPath
     *            the properties path
     * @param clazz
     *            the clazz
     * @throws BindingException
     *             the binding exception
     */
    public static synchronized void bind(String propertiesPath, Class clazz) throws BindingException
    {
        if (em == null)
        {
            em = PersistenceService.getEM(emf, propertiesPath, clazz.getName());
        }
        onBind(clazz);
    }

    /**
     * Unbind.
     */
    public static synchronized void unbind()
    {
        if (emf != null && emf.isOpen())
        {
            emf.close();
            emf = null;

        }
        if (em != null && em.isOpen())
        {
            em.close();
            em = null;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.core.KunderaEntity#leftJoin(java.lang.Class,
     * java.lang.String, java.lang.String[])
     */
    // public final List leftJoin(Class clazz, String joinColumn, String...
    // columnTobeFetched)
    // {
    // List<T> finalResult = new ArrayList();
    // List<T> leftTable = em.createQuery("Select p from " +
    // this.getClass().getSimpleName() + " p").getResultList();
    // EntityType leftEntity = ((MetamodelImpl)
    // em.getMetamodel()).entity(this.getClass());
    // Attribute attribute = leftEntity.getAttribute(joinColumn);
    // Field field = (Field) attribute.getJavaMember();
    //
    // for (T obj : leftTable)
    // {
    // List rightTable = em
    // .createQuery(
    // "Select p from " + clazz.getSimpleName() + " p where p." + joinColumn + "
    // = :columnValue")
    // .setParameter("columnValue", PropertyAccessorHelper.getObject(obj,
    // field)).getResultList();
    // if (!rightTable.isEmpty())
    // {
    // finalResult.add(obj);
    // }
    // }
    // return finalResult;
    // }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.core.KunderaEntity#query(java.lang.String)
     */
    public List<T> query(String query)
    {
        return em.createQuery(query).getResultList();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.core.KunderaEntity#query(java.lang.String,
     * com.impetus.core.QueryType)
     */
    public List<T> query(String query, QueryType type)
    {
        switch (type)
        {
        case JPQL:
            return query(query);

        case NATIVE:
            return nativeQuery(query);

        default:
            throw new KunderaException("invalid query type");
        }
    }

    /**
     * Native query.
     *
     * @param query
     *            the query
     * @return the list
     */
    private List<T> nativeQuery(String query)
    {
        return em.createNativeQuery(query).getResultList();
    }

}
