/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityReader;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery.FilterClause;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * @author vivek.mishra
 * Test implementation for {@link Query} interface.
 *
 */
public class CoreQuery<E> extends QueryImpl<E>
{

    public CoreQuery(final KunderaQuery kunderaQuery, PersistenceDelegator persistenceDelegator, final KunderaMetadata kunderaMetadata)
    {
        super(kunderaQuery, persistenceDelegator, kunderaMetadata);
    }


    public List getResutList()
    {
        return super.getResultList();
    }
    
    @Override
    protected List<Object> populateEntities(EntityMetadata m, Client client)
    {
        // Only find by id queries supported.
        
        EntityMetadata entityMetadata = getEntityMetadata();
        Object value = null;
        for(Object clause: getKunderaQuery().getFilterClauseQueue())
        {
            if(clause instanceof FilterClause)
            {
                String property = ((FilterClause)clause).getProperty();
                String condition = ((FilterClause)clause).getCondition();
                value = ((FilterClause)clause).getValue().get(0);
            }
            
            
            
        }

        Object result = client.find(m.getEntityClazz(), value);
        List results = new ArrayList();
        if(result != null)
        {
            results.add(result);
        }
        return results;
    }

    public String getLuceneQuery()
    {
        return KunderaCoreUtils.getLuceneQueryFromJPAQuery(kunderaQuery, kunderaMetadata);
    }
    
    /*public Set<String> fetchByLuceneQuery()
    {
        return fetchDataFromLucene(getEntityMetadata().getEntityClazz(), persistenceDelegeator.getClient(getEntityMetadata()));
    }*/
    
    public List<Object> populateUsingLucene()
    {
        return populateUsingLucene(getEntityMetadata(),persistenceDelegeator.getClient(getEntityMetadata()),new ArrayList(),new String[]{});
    }
    
    
    @Override
    protected List<Object> recursivelyPopulateEntities(EntityMetadata m, Client client)
    {
//        setRelationEntities(enhanceEntities, client, m)
        List results = populateUsingLucene(m,client,new ArrayList(),new String[]{});
        return setRelationEntities(results, client, m);
    }

    @Override
    protected EntityReader getReader()
    {
        return new CoreTestEntityReader(kunderaMetadata);
    }

    @Override
    protected int onExecuteUpdate()
    {
        return onUpdateDeleteEvent();
    }

    @Override
    public void close()
    {
        // Do nothing.
        
    }

    @Override
    public Iterator<E> iterate()
    {
        return null;
    }
    
    protected String[] getColumns(final String[] columns, final EntityMetadata m)
    {
        return super.getColumns(columns, m);
    }


    @Override
    protected List findUsingLucene(EntityMetadata m, Client client)
    {
        return populateEntities(m, client);
    }


   

}
