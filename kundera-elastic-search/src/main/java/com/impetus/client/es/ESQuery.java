/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.es;

import java.util.Iterator;
import java.util.List;

import javax.persistence.Query;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;

import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.query.QueryImpl;

/**
 * @author vivek.mishra Implementation of query interface {@link Query}.
 */

public class ESQuery<E> extends QueryImpl
{

    /**
     * Constructor using fields.
     * 
     * @param query
     *            jpa query.
     * @param persistenceDelegator
     *            persistence delegator.
     */
    public ESQuery(String query, PersistenceDelegator persistenceDelegator)
    {
        super(query, persistenceDelegator);
    }

    public ESQuery(String query, KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator)
    {
        super(query, persistenceDelegator);
        this.kunderaQuery = kunderaQuery;

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#populateEntities(com.impetus.kundera
     * .metadata.model.EntityMetadata, com.impetus.kundera.client.Client)
     */
    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        
        MetamodelImpl metaModel = (MetamodelImpl) KunderaMetadata.INSTANCE.getApplicationMetadata().getMetamodel(
                m.getPersistenceUnit());
        EntityType entity = metaModel.entity(m.getEntityClazz());
        
        FilterBuilder preIntraFilter = null;
        FilterBuilder preInterFilter = null;
        String interFilter = null;
        
        // Select p from Person p where p.age= 32 and p.name= 'vivek';
        
        for (Object o : getKunderaQuery().getFilterClauseQueue())
        {
            if (o instanceof FilterClause)
            {
                FilterClause clause = ((FilterClause) o);
                FilterBuilder currentFilter = getFilter(clause,m,entity);
                if(interFilter != null)
                {
                    if(interFilter.equalsIgnoreCase("and"))
                    {
                        preInterFilter = new AndFilterBuilder(preIntraFilter,currentFilter);
                    } else if(interFilter.equalsIgnoreCase("or"))
                    {
                        preInterFilter = new OrFilterBuilder(preIntraFilter,currentFilter);
                    }
                }

                preIntraFilter = currentFilter;
            } else 
            {
                String opr = o.toString();
                interFilter=opr;
            }
        }
        
        
        return ((ESClient)client).executeQuery(preInterFilter != null ? preInterFilter : preIntraFilter, m);
        
//        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.query.QueryImpl#recursivelyPopulateEntities(com.impetus
     * .kundera.metadata.model.EntityMetadata,
     * com.impetus.kundera.client.Client)
     */
    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
        List result = populateEntities(m, client);
        return setRelationEntities(result, client, m);
//        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#getReader()
     */
    @Override
    protected EntityReader getReader()
    {
        return new ESEntityReader();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.query.QueryImpl#onExecuteUpdate()
     */
    @Override
    protected int onExecuteUpdate()
    {
        return 0;
    }

    @Override
    public void close()
    {

    }

    @Override
    public Iterator<E> iterate()
    {
        return null;
    }

    private FilterBuilder getFilter(FilterClause clause ,final EntityMetadata metadata, final EntityType entityType)
    {
        String condition = clause.getCondition();
        Object value = clause.getValue();
        String name = clause.getProperty();
        
        FilterBuilder filterBuilder = null;
        if (condition.equals("="))
        {
            filterBuilder = new TermFilterBuilder(name,value);
        }
        else if (condition.equals(">"))
        {
            filterBuilder = new RangeFilterBuilder(name).gt(value);
        }
        else if (condition.equals("<"))
        {
            filterBuilder = new RangeFilterBuilder(name).lt(value);
        }
        else if (condition.equals(">="))
        {
            filterBuilder = new RangeFilterBuilder(name).gte(value);
        }
        else if (condition.equals("<="))
        {
            filterBuilder = new RangeFilterBuilder(name).lte(value);
        }
    
        return filterBuilder;
    }

}



