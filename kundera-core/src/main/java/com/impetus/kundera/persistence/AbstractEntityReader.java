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
package com.impetus.kundera.persistence;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.FetchType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * The Class AbstractEntityReader.
 * 
 * @author vivek.mishra
 */
public class AbstractEntityReader
{

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(AbstractEntityReader.class);

    /** The lucene query from jpa query. */
    protected String luceneQueryFromJPAQuery;

    AssociationBuilder associationBuilder;

    /**
     * Retrieves an entity from ID
     * 
     * @param primaryKey
     * @param m
     * @param client
     * @return
     */
    protected EnhanceEntity findById(Object primaryKey, EntityMetadata m, Client client)
    {
        try
        {
            Object o = client.find(m.getEntityClazz(), primaryKey);

            if (o == null)
            {
                // No entity found
                return null;
            }
            else
            {
                return o instanceof EnhanceEntity ? (EnhanceEntity) o : new EnhanceEntity(o, getId(o, m), null);
            }
        }
        catch (Exception e)
        {
            throw new EntityReaderException(e);
        }
    }

    /**
     * Recursively fetches associated entities for a given <code>entity</code> 
     * @param entity
     * @param relationsMap
     * @param client
     * @param m
     * @param pd
     * @return
     */
    public Object recursivelyFindEntities(Object entity, Map<String, Object> relationsMap, EntityMetadata m,
            PersistenceDelegator pd)
    {
        Object entityId = PropertyAccessorHelper.getId(entity, m);        
        associationBuilder = new AssociationBuilder();
        
        for (Relation relation : m.getRelations())
        {   
			// If fetch type is LAZY, just populate proxy object and skip to next relation
            FetchType fetchType = relation.getFetchType();
            
			if (fetchType.equals(FetchType.LAZY)) {
				associationBuilder.setProxyRelationObject(entity, relationsMap, m, pd, entityId, relation);
				continue;
			}				
          
			//An EAGER fetch type means, you have to set "actual" or concrete associated object to original entity
			associationBuilder.setConcreteRelationObject(entity, relationsMap, m, pd, entityId,
					relation);     
            
        }
        return entity;
    }    

    /**
     * On association using lucene.
     * 
     * @param m
     *            the m
     * @param client
     *            the client
     * @param ls
     *            the ls
     * @return the list
     */
    protected List<EnhanceEntity> onAssociationUsingLucene(EntityMetadata m, Client client, List<EnhanceEntity> ls)
    {
        Set<String> rSet = fetchDataFromLucene(client);
        List resultList = client.findAll(m.getEntityClazz(), null, rSet.toArray(new String[] {}));
        return m.getRelationNames() != null && !m.getRelationNames().isEmpty() ? resultList : transform(m, ls,
                resultList);
    }

    /**
     * Transform.
     * 
     * @param m
     *            the m
     * @param ls
     *            the ls
     * @param resultList
     *            the result list
     * @return the list
     */
    protected List<EnhanceEntity> transform(EntityMetadata m, List<EnhanceEntity> ls, List resultList)
    {
        if ((ls == null || ls.isEmpty()) && resultList != null && !resultList.isEmpty())
        {
            ls = new ArrayList<EnhanceEntity>(resultList.size());
        }
        for (Object r : resultList)
        {
            EnhanceEntity e = new EnhanceEntity(r, getId(r, m), null);
            ls.add(e);
        }
        return ls;
    }

    /**
     * Fetch data from lucene.
     * 
     * @param client
     *            the client
     * @return the sets the
     */
    protected Set<String> fetchDataFromLucene(Client client)
    {
        // use lucene to query and get Pk's only.
        // go to client and get relation with values.!
        // populate EnhanceEntity
        Map<String, Object> results = client.getIndexManager().search(luceneQueryFromJPAQuery);
        Set rSet = new HashSet(results.values());
        return rSet;
    }

    /**
     * Gets the id.
     * 
     * @param entity
     *            the entity
     * @param metadata
     *            the metadata
     * @return the id
     */
    protected Object getId(Object entity, EntityMetadata metadata)
    {
        try
        {
            return PropertyAccessorHelper.getId(entity, metadata);
        }
        catch (PropertyAccessException e)
        {
            log.error("Error while Getting ID, Caused by: ", e);
            throw new EntityReaderException("Error while Getting ID for entity " + entity, e);
        }

    }
    
    
}