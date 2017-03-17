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
package com.impetus.client.rdbms.query;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.persistence.Parameter;
import javax.persistence.Query;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EmbeddableType;
import javax.persistence.metamodel.EntityType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.rdbms.HibernateClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.MetadataUtils;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.attributes.AbstractAttribute;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.QueryHandlerException;
import com.impetus.kundera.query.QueryImpl;
import com.impetus.kundera.utils.ReflectUtils;

/**
 * The Class RDBMSQuery.
 * 
 * @author vivek.mishra
 */
public class RDBMSQuery extends QueryImpl
{
    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(RDBMSQuery.class);

    /** The reader. */
    private RDBMSEntityReader reader;

    /**
     * Instantiates a new RDBMS query.
     * 
     * @param query
     *            the query
     * @param kunderaQuery
     *            the kundera query
     * @param persistenceDelegator
     *            the persistence delegator
     * @param persistenceUnits
     *            the persistence units
     */
    public RDBMSQuery(KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator,
            final KunderaMetadata kunderaMetadata)
    {
        super(kunderaQuery, persistenceDelegator, kunderaMetadata);
    }

    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        // retrieve
        if (log.isDebugEnabled())
        {
            log.debug("On handleAssociation() retrieve associations ");
        }

        initializeReader();

        List<EnhanceEntity> ls = getReader().populateRelation(m, client, this.maxResult);

        return setRelationEntities(ls, client, m);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#populateEntities(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        if (log.isDebugEnabled())
        {
            log.debug("on start of fetching non associated entities");
        }
        List<Object> result = new ArrayList<Object>();

        initializeReader();

        try
        {
            if (MetadataUtils.useSecondryIndex(((ClientBase) client).getClientMetadata()))
            {
                List<String> relations = new ArrayList<String>();
                if (kunderaQuery.isNative())
                {
                    return ((HibernateClient) client).find(getJPAQuery(), relations, m);

                }
                else
                {
                    return ((HibernateClient) client).findByQuery(getJPAQuery(), getParamaters(), firstResult, maxResult);

                }
            }
            else
            {
                return populateUsingLucene(m, client, result, null);
            }
        }
        catch (Exception e)
        {
            log.error("Error during query execution, Caused by: {}.", e);
            throw new QueryHandlerException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected RDBMSEntityReader getReader()
    {
        if (reader == null)
        {
            reader = new RDBMSEntityReader(getJPAQuery(), kunderaQuery, kunderaMetadata);
        }
        return reader;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#onExecuteUpdate()
     */
    @Override
    protected int onExecuteUpdate()
    {
        EntityMetadata m = getEntityMetadata();

        Client client = persistenceDelegeator.getClient(m);
        return ((HibernateClient) client).onExecuteUpdate(getJPAQuery(), getParamaters(), firstResult, maxResult);
    }

    /**
     * Initializes reader with conditions and filter in case for JPA/Named query
     * only!
     * 
     */
    private void initializeReader()
    {
        boolean isNative = kunderaQuery.isNative();

        if (!isNative)
        {
            getReader().setConditions(getKunderaQuery().getFilterClauseQueue());

            getReader().setFilter(getKunderaQuery().getFilter());
        }
    }

    @Override
    public void close()
    {
        this.reader = null;
    }

    @Override
    public Iterator iterate()
    {
        if (kunderaQuery.isNative())
        {
            throw new UnsupportedOperationException("Iteration not supported over native queries");
        }

        initializeReader();
        EntityMetadata m = getEntityMetadata();
        Client client = persistenceDelegeator.getClient(m);
        return new ResultIterator((HibernateClient) client, m, persistenceDelegeator,
                getFetchSize() != null ? getFetchSize() : this.maxResult,
                ((RDBMSEntityReader) getReader()).getSqlQueryFromJPA(m, m.getRelationNames(), null));
    }

    /**
     * Gets the column list.
     * 
     * @param m
     *            the m
     * @param results
     *            the results
     * @return the column list
     */
    List<String> getColumnList(EntityMetadata m, String[] results, EmbeddableType compoundKey)
    {
        List<String> columns = new ArrayList<String>();
        if (results != null && results.length > 0)
        {
            MetamodelImpl metaModel = (MetamodelImpl) kunderaMetadata.getApplicationMetadata().getMetamodel(
                    m.getPersistenceUnit());
            EntityType entity = metaModel.entity(m.getEntityClazz());

            for (int i = 1; i < results.length; i++)
            {
                if (results[i] != null)
                {
                    Attribute attribute = entity.getAttribute(results[i]);

                    if (attribute == null)
                    {
                        throw new QueryHandlerException("column type is null for: " + results);
                    }
                    else if (m.getIdAttribute().equals(attribute) && compoundKey != null)
                    {
                        Field[] fields = m.getIdAttribute().getBindableJavaType().getDeclaredFields();
                        for (Field field : fields)
                        {
                            if (!ReflectUtils.isTransientOrStatic(field))
                            {
                                Attribute compositeColumn = compoundKey.getAttribute(field.getName());
                                columns.add(((AbstractAttribute) compositeColumn).getJPAColumnName());
                            }
                        }
                    }
                    else
                    {
                        columns.add(((AbstractAttribute) attribute).getJPAColumnName());
                    }
                }
            }
            return columns;
        }

        if (log.isInfoEnabled())
        {
            log.info("No record found, returning null.");
        }
        return null;
    }

    private Map<Parameter, Object> getParamaters()
    {
        Map<Parameter, Object> parameterMap = new HashMap<Parameter, Object>();

        if (getKunderaQuery().getParameters() == null || getKunderaQuery().getParameters().isEmpty())
        {
            return parameterMap;
        }

        for (Parameter parameter : getKunderaQuery().getParameters())
        {
            parameterMap.put(parameter, getKunderaQuery().getClauseValue(parameter));
        }
        return parameterMap;
    }

    @Override
    protected List findUsingLucene(EntityMetadata m, Client client)
    {
        return ((HibernateClient) client).findByQuery(getJPAQuery(), getParamaters(), firstResult, maxResult);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#setFirstResult(int)
     */
    @Override
    public Query setFirstResult(int firstResult)
    {
        return super.setFirstResult(firstResult);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#setMaxResults(int)
     */
    @Override
    public Query setMaxResults(int maxResult)
    {
        return super.setMaxResults(maxResult);
    }

}
