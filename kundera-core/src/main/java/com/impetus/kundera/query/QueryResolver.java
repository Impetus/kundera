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
import java.lang.reflect.InvocationTargetException;

import javax.persistence.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
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
    private static Log log = LogFactory.getLog(QueryResolver.class);

    /** The kundera query. */
    KunderaQuery kunderaQuery;

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
    public Query getQueryImplementation(String jpaQuery, PersistenceDelegator persistenceDelegator)
    {
        if (jpaQuery == null)
        {
            throw new QueryHandlerException("Query String should not be null ");
        }
        kunderaQuery = new KunderaQuery();
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        String mappedQuery = appMetadata.getQuery(jpaQuery);
        boolean isNative = appMetadata.isNative(jpaQuery);
//        String pu = null;
        EntityMetadata m = null;
        // In case of named native query
        if (!isNative)
        {
            KunderaQueryParser parser = new KunderaQueryParser(kunderaQuery, mappedQuery != null ? mappedQuery
                    : jpaQuery);

            parser.parse();

            kunderaQuery.postParsingInit();
//            pu = kunderaQuery.getPersistenceUnit();
            m = kunderaQuery.getEntityMetadata();
        }
        else
        {
            Class mappedClass = appMetadata.getMappedClass(jpaQuery);

//            pu = appMetadata.getMappedPersistenceUnit(mappedClass).get(0);
            m = KunderaMetadataManager.getEntityMetadata(mappedClass);
        }

//        PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(pu);

        Query query = null;

        try
        {
            query = getQuery(jpaQuery, persistenceDelegator, m);
        }
        catch (SecurityException e)
        {
            log.error(e.getMessage());
            throw new QueryHandlerException(e);
        }
        catch (IllegalArgumentException e)
        {
            log.error(e.getMessage());
            throw new QueryHandlerException(e);
        }
        catch (ClassNotFoundException e)
        {
            log.error(e.getMessage());
            throw new QueryHandlerException(e);
        }
        catch (NoSuchMethodException e)
        {
            log.error(e.getMessage());
            throw new QueryHandlerException(e);
        }
        catch (InstantiationException e)
        {
            log.error(e.getMessage());
            throw new QueryHandlerException(e);
        }
        catch (IllegalAccessException e)
        {
            log.error(e.getMessage());
            throw new QueryHandlerException(e);
        }
        catch (InvocationTargetException e)
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
    public Query getQuery(String jpaQuery, PersistenceDelegator persistenceDelegator, EntityMetadata m)
            throws ClassNotFoundException, SecurityException, NoSuchMethodException, IllegalArgumentException,
            InstantiationException, IllegalAccessException, InvocationTargetException
    {
        Query query;
        Class clazz = persistenceDelegator.getClient(m).getQueryImplementor();

        @SuppressWarnings("rawtypes")
        Constructor constructor = clazz.getConstructor(String.class, KunderaQuery.class, PersistenceDelegator.class);
        query = (Query) constructor.newInstance(jpaQuery, kunderaQuery, persistenceDelegator);

        return query;
    }
}
