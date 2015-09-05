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
package com.impetus.kundera.query;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.Constants;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;

/**
 * The Class QueryResolver.
 * 
 * @author amresh.singh
 * 
 */
public class QueryResolver
{

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(QueryResolver.class);

    /**
     * Gets the query implementation.
     * 
     * @param jpaQuery
     *            the jpa query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param persistenceUnits
     *            the persistence units
     * @return the query implementation
     */
    public Query getQueryImplementation(String jpaQuery, PersistenceDelegator persistenceDelegator, Class mappedClass,
            boolean isNative, final KunderaMetadata kunderaMetadata)
    {
        if (jpaQuery == null)
        {
            throw new QueryHandlerException("Query String should not be null ");
        }

        if (jpaQuery.trim().endsWith(Constants.SEMI_COLON))
        {
            throw new QueryHandlerException("unexpected char: ';' in query [ " + jpaQuery + " ]");
        }

        KunderaQuery kunderaQuery = null;
        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();
        String mappedQuery = appMetadata.getQuery(jpaQuery);

        isNative = mappedQuery != null ? appMetadata.isNative(jpaQuery) : isNative;

        EntityMetadata m = null;

        // In case of named native query
        if (!isNative)
        {
            kunderaQuery = new KunderaQuery(mappedQuery != null ? mappedQuery : jpaQuery, kunderaMetadata);
            KunderaQueryParser parser = new KunderaQueryParser(kunderaQuery);

            parser.parse();

            kunderaQuery.postParsingInit();
            m = kunderaQuery.getEntityMetadata();
        }
        else
        {
            // Means if it is a namedNativeQuery.
            if (appMetadata.isNative(jpaQuery))
            {
                mappedClass = appMetadata.getMappedClass(jpaQuery);
            }

            kunderaQuery = new KunderaQuery(jpaQuery, kunderaMetadata);

            kunderaQuery.isNativeQuery = true;

            m = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, mappedClass);

            Field entityClazzField = null;
            try
            {
                entityClazzField = kunderaQuery.getClass().getDeclaredField("entityClass");
                if (entityClazzField != null && !entityClazzField.isAccessible())
                {
                    entityClazzField.setAccessible(true);
                }

                entityClazzField.set(kunderaQuery, mappedClass);
            }
            catch (Exception e)
            {
                log.error(e.getMessage());
                throw new QueryHandlerException(e);
            }
        }

        Query query = null;

        try
        {
            query = getQuery(jpaQuery, persistenceDelegator, m, kunderaQuery, kunderaMetadata);
        }
        catch (Exception e)
        {
            log.error(e.getMessage());
            throw new QueryHandlerException(e);
        }
        return query;
    }

    /**
     * Gets the query instance.
     * 
     * @param jpaQuery
     *            the jpa query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param persistenceUnits
     *            the persistence units
     * @return the query
     * @throws ClassNotFoundException
     *             the class not found exception
     * @throws SecurityException
     *             the security exception
     * @throws NoSuchMethodException
     *             the no such method exception
     * @throws IllegalArgumentException
     *             the illegal argument exception
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     * @throws InvocationTargetException
     *             the invocation target exception
     */
    private Query getQuery(String jpaQuery, PersistenceDelegator persistenceDelegator, EntityMetadata m,
            KunderaQuery kunderaQuery, final KunderaMetadata kunderaMetadata) throws ClassNotFoundException,
            SecurityException, NoSuchMethodException, IllegalArgumentException, InstantiationException,
            IllegalAccessException, InvocationTargetException
    {
        Query query;

        Class clazz = persistenceDelegator.getClient(m).getQueryImplementor();

        @SuppressWarnings("rawtypes")
        Constructor constructor = clazz.getConstructor(KunderaQuery.class, PersistenceDelegator.class,
                KunderaMetadata.class);
        query = (Query) constructor.newInstance(kunderaQuery, persistenceDelegator, kunderaMetadata);

        return query;
    }

    /**
     * 
     * @param jpaQuery
     * @param queryClazz
     * @param persistenceDelegator
     * @param metadata
     * @return
     */
    public Query getQueryImplementation(String jpaQuery, Class queryClazz,
            final PersistenceDelegator persistenceDelegator, EntityMetadata metadata, String persistenceUnit)
    {
        KunderaQuery kunderaQuery = new KunderaQuery(jpaQuery, persistenceDelegator.getKunderaMetadata());
        kunderaQuery.isNativeQuery = true;
        kunderaQuery.setPersistenceUnit(persistenceUnit);

        try
        {
            if (metadata != null)
            {
                Field entityClazzField = kunderaQuery.getClass().getDeclaredField("entityClass");
                if (entityClazzField != null && !entityClazzField.isAccessible())
                {
                    entityClazzField.setAccessible(true);
                }

                entityClazzField.set(kunderaQuery, metadata.getEntityClazz());
            }
        }
        catch (Exception e)
        {
            log.error(e.getMessage());
            throw new QueryHandlerException(e);
        }

        Query query = null;

        try
        {
            Constructor constructor = queryClazz.getConstructor(KunderaQuery.class, PersistenceDelegator.class,
                    KunderaMetadata.class);
            query = (Query) constructor.newInstance(kunderaQuery, persistenceDelegator,
                    persistenceDelegator.getKunderaMetadata());
        }
        catch (Exception e)
        {
            log.error(e.getMessage());
            throw new QueryHandlerException(e);
        }
        return query;

    }
}
