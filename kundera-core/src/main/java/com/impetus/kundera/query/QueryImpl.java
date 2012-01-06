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
package com.impetus.kundera.query;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Parameter;
import javax.persistence.Query;
import javax.persistence.TemporalType;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.EnhanceEntity;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.persistence.handler.impl.EntitySaveGraph;
import com.impetus.kundera.property.PropertyAccessException;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.query.exception.QueryHandlerException;

/**
 * The Class QueryImpl.
 * 
 * @author vivek.mishra
 */
public abstract class QueryImpl implements Query
{

    /** The query. */
    protected String query;

    /** The kundera query. */
    protected KunderaQuery kunderaQuery;

    /** The persistence delegeator. */
    protected PersistenceDelegator persistenceDelegeator;

    private static Log log = LogFactory.getLog(QueryImpl.class);
    /**
     * Instantiates a new query impl.
     *
     * @param query the query
     * @param persistenceDelegator the persistence delegator
     * @param persistenceUnits the persistence units
     */
    public QueryImpl(String query, PersistenceDelegator persistenceDelegator, String... persistenceUnits)
    {

        this.query = query;
        this.persistenceDelegeator = persistenceDelegator;
    }

    /**
     * Gets the jPA query.
     * 
     * @return the jPA query
     */
    public String getJPAQuery()
    {
        return query;
    }

    /**
     * Gets the kundera query.
     *
     * @return the kunderaQuery
     */
    public KunderaQuery getKunderaQuery()
    {
        return kunderaQuery;
    }

    /**
     * Sets the kundera query.
     *
     * @param kunderaQuery the kunderaQuery to set
     */
    public void setKunderaQuery(KunderaQuery kunderaQuery)
    {
        this.kunderaQuery = kunderaQuery;
    }

    /* @see javax.persistence.Query#executeUpdate() */
    /* (non-Javadoc)
     * @see javax.persistence.Query#executeUpdate()
     */
    @Override
    public int executeUpdate()
    {
        throw new NotImplementedException("TODO");
    }

    /* @see javax.persistence.Query#getResultList() */
    /* (non-Javadoc)
     * @see javax.persistence.Query#getResultList()
     */
    @Override
    public List<?> getResultList()
    {
        log.debug("JPA Query: " + query);
        try
        {
            // TODO need to look what are the values for given from, select and
            // where clause.
            EntityMetadata m = kunderaQuery.getEntityMetadata();
            Client client = persistenceDelegeator.getClient(m);

            // get Graph
            List<EntitySaveGraph> graphs = persistenceDelegeator.getGraph(m.getEntityClazz().newInstance(), m);
            // Get relations.
            Map<Boolean, List<String>> relationHolder = getRelations(graphs, m.getEntityClazz());
            List<String> relationNames = relationHolder.values().iterator().next();
            boolean isParent = relationHolder.keySet().iterator().next();

            if (relationNames.isEmpty())
            {
                // There is no association so simply return list of entities.
                return populateEntities(m, client);

            }
            else

            {
              return  handleAssociations(m, client, graphs, relationNames, isParent);
            }

        }
        catch (InstantiationException e)
        {
            log.error("error while returing query result:" + e.getMessage());
            throw new QueryHandlerException(e.getMessage());
        }
        catch (IllegalAccessException e)
        {
            log.error("error while returing query result:" + e.getMessage());
            throw new QueryHandlerException(e.getMessage());
        } // Query is parsed.
          // get Graph
          // If there is any relation and entity is not parent,
          // get client from persistenceDelegator and find that object.
          // set that object in graph
          // Populate child entities according to graph.
          // if entity is parent pass it as foreign key id for client
          // if entity is not parent then pass retrieved relation key value to
          // specific client for find by id.

//        return null;

    }

    /**
     * Gets the persistence delegeator.
     *
     * @return the persistenceDelegeator
     */
    public PersistenceDelegator getPersistenceDelegeator()
    {
        return persistenceDelegeator;
    }

    /**
     * Sets the persistence delegeator.
     *
     * @param persistenceDelegeator the persistenceDelegeator to set
     */
    public void setPersistenceDelegeator(PersistenceDelegator persistenceDelegeator)
    {
        this.persistenceDelegeator = persistenceDelegeator;
    }

    /* @see javax.persistence.Query#getSingleResult() */
    /* (non-Javadoc)
     * @see javax.persistence.Query#getSingleResult()
     */
    @Override
    public Object getSingleResult()
    {
        throw new NotImplementedException("TODO");
    }

    /* @see javax.persistence.Query#setFirstResult(int) */
    /* (non-Javadoc)
     * @see javax.persistence.Query#setFirstResult(int)
     */
    @Override
    public Query setFirstResult(int startPosition)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * @see
     * javax.persistence.Query#setFlushMode(javax.persistence.FlushModeType)
     */
    /* (non-Javadoc)
     * @see javax.persistence.Query#setFlushMode(javax.persistence.FlushModeType)
     */
    @Override
    public Query setFlushMode(FlushModeType flushMode)
    {
        throw new NotImplementedException("TODO");
    }

    /* @see javax.persistence.Query#setHint(java.lang.String, java.lang.Object) */
    /* (non-Javadoc)
     * @see javax.persistence.Query#setHint(java.lang.String, java.lang.Object)
     */
    @Override
    public Query setHint(String hintName, Object value)
    {
        throw new NotImplementedException("TODO");
    }

    /* @see javax.persistence.Query#setMaxResults(int) */
    /* (non-Javadoc)
     * @see javax.persistence.Query#setMaxResults(int)
     */
    @Override
    public Query setMaxResults(int maxResult)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * @see javax.persistence.Query#setParameter(java.lang.String,
     * java.lang.Object)
     */
    /* (non-Javadoc)
     * @see javax.persistence.Query#setParameter(java.lang.String, java.lang.Object)
     */
    @Override
    public Query setParameter(String name, Object value)
    {
        kunderaQuery.setParameter(name, value.toString());
        return this;
    }

    /* @see javax.persistence.Query#setParameter(int, java.lang.Object) */
    /* (non-Javadoc)
     * @see javax.persistence.Query#setParameter(int, java.lang.Object)
     */
    @Override
    public Query setParameter(int position, Object value)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * @see javax.persistence.Query#setParameter(java.lang.String,
     * java.util.Date, javax.persistence.TemporalType)
     */
    /* (non-Javadoc)
     * @see javax.persistence.Query#setParameter(java.lang.String, java.util.Date, javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(String name, Date value, TemporalType temporalType)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * @see javax.persistence.Query#setParameter(java.lang.String,
     * java.util.Calendar, javax.persistence.TemporalType)
     */
    /* (non-Javadoc)
     * @see javax.persistence.Query#setParameter(java.lang.String, java.util.Calendar, javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(String name, Calendar value, TemporalType temporalType)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * @see javax.persistence.Query#setParameter(int, java.util.Date,
     * javax.persistence.TemporalType)
     */
    /* (non-Javadoc)
     * @see javax.persistence.Query#setParameter(int, java.util.Date, javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(int position, Date value, TemporalType temporalType)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * @see javax.persistence.Query#setParameter(int, java.util.Calendar,
     * javax.persistence.TemporalType)
     */
    /* (non-Javadoc)
     * @see javax.persistence.Query#setParameter(int, java.util.Calendar, javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(int position, Calendar value, TemporalType temporalType)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getMaxResults()
     */
    @Override
    public int getMaxResults()
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getFirstResult()
     */
    @Override
    public int getFirstResult()
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getHints()
     */
    @Override
    public Map<String, Object> getHints()
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setParameter(javax.persistence.Parameter,
     * java.lang.Object)
     */
    @Override
    public <T> Query setParameter(Parameter<T> paramParameter, T paramT)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setParameter(javax.persistence.Parameter,
     * java.util.Calendar, javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(Parameter<Calendar> paramParameter, Calendar paramCalendar, TemporalType paramTemporalType)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setParameter(javax.persistence.Parameter,
     * java.util.Date, javax.persistence.TemporalType)
     */
    @Override
    public Query setParameter(Parameter<Date> paramParameter, Date paramDate, TemporalType paramTemporalType)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameters()
     */
    @Override
    public Set<Parameter<?>> getParameters()
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameter(java.lang.String)
     */
    @Override
    public Parameter<?> getParameter(String paramString)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameter(java.lang.String,
     * java.lang.Class)
     */
    @Override
    public <T> Parameter<T> getParameter(String paramString, Class<T> paramClass)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameter(int)
     */
    @Override
    public Parameter<?> getParameter(int paramInt)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameter(int, java.lang.Class)
     */
    @Override
    public <T> Parameter<T> getParameter(int paramInt, Class<T> paramClass)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#isBound(javax.persistence.Parameter)
     */
    @Override
    public boolean isBound(Parameter<?> paramParameter)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.persistence.Query#getParameterValue(javax.persistence.Parameter)
     */
    @Override
    public <T> T getParameterValue(Parameter<T> paramParameter)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameterValue(java.lang.String)
     */
    @Override
    public Object getParameterValue(String paramString)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getParameterValue(int)
     */
    @Override
    public Object getParameterValue(int paramInt)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getFlushMode()
     */
    @Override
    public FlushModeType getFlushMode()
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#setLockMode(javax.persistence.LockModeType)
     */
    @Override
    public Query setLockMode(LockModeType paramLockModeType)
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#getLockMode()
     */
    @Override
    public LockModeType getLockMode()
    {
        throw new NotImplementedException("TODO");
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.persistence.Query#unwrap(java.lang.Class)
     */
    @Override
    public <T> T unwrap(Class<T> paramClass)
    {
        throw new NotImplementedException("TODO");
    }

    /**
     * Gets the relations.
     *
     * @param graphs the graphs
     * @param clazz the clazz
     * @return the relations
     */
    protected Map<Boolean, List<String>> getRelations(List<EntitySaveGraph> graphs, Class clazz)
    {
        List<String> relationNames = new ArrayList<String>(graphs.size());
        boolean isParent = false;
        Map<Boolean, List<String>> relationHolder = new HashMap<Boolean, List<String>>(1);
        // TODO need to check if there is any relation?
        for (EntitySaveGraph g : graphs)
        {
            if (clazz.equals(g.getParentClass()))
            {
                isParent = true;
                // Means entity is parent
            }

            if (g.getfKeyName() != null)
            {
                relationNames.add(g.getfKeyName());
            }
        }

        relationHolder.put(isParent, relationNames);
        return relationHolder;

    }

    /**
     * Handle graph.
     *
     * @param enhanceEntities the enhance entities
     * @param graphs the graphs
     */
    protected List<Object> handleGraph(List<EnhanceEntity> enhanceEntities, List<EntitySaveGraph> graphs)
    {
        // Enhance entities can contain or may not contain relation.
        // if it contain a relation means it is a child
        // if it does not then it means it is a parent.
        List<Object> result = null;
        Map<Object, Object> relationalValues = new HashMap<Object, Object>();
        for (EnhanceEntity e : enhanceEntities)
        {
            if(result == null)
            {
                result = new ArrayList<Object>(enhanceEntities.size());
            }
            try
            {
                result.add(computeGraph(e, graphs, relationalValues));
            }
            catch (Exception e1)
            {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
        return result;
    }

    /**
     * Compute graph.
     *
     * @param e the e
     * @param graphs the graphs
     * @return the object
     * @throws Exception the exception
     */
    private Object computeGraph(EnhanceEntity e, List<EntitySaveGraph> graphs, Map<Object, Object> collectionHolder) throws Exception
    {

        Client childClient = null;
        Class<?> childClazz = null;
        EntityMetadata childMetadata = null;

        if (e.getRelations() != null)
        {
            // means it is holding associations
            // read graph for all the association
            for (EntitySaveGraph g : graphs)
            {

                String relationName = g.getfKeyName();
                Object relationalValue = e.getRelations().get(relationName);
                childClazz = g.getParentClass();
                Field f = g.getProperty();
                if(!collectionHolder.containsKey(relationalValue))
                {
                    childMetadata = persistenceDelegeator.getMetadata(childClazz);
                    childClient = persistenceDelegeator.getClient(childMetadata);
                    Object child = childClient.find(childClazz, relationalValue.toString());
                    f = g.getProperty();
                    collectionHolder.put(relationalValue, child);
                    // If entity is holding association it means it can not be a
                    // collection.
                }
                    PropertyAccessorHelper.set(e.getEntity(), f, collectionHolder.get(relationalValue));
                }
            }
        else
        {
            // means it is parent
            for (EntitySaveGraph g : graphs)
            {
                childClazz = g.getChildClass();
                childMetadata = persistenceDelegeator.getMetadata(childClazz);
                childClient = persistenceDelegeator.getClient(childMetadata);
                String relationName = g.getfKeyName();
                String relationalValue = e.getEntityId();
                Field f = g.getProperty();
                if(!collectionHolder.containsKey(relationalValue))
                {
                    // create a finder and pass metadata, relationName,
                    // relationalValue.
                    List<Object> childs = childClient.find(relationName, relationalValue, childMetadata);
                    // pass this entity id as a value to be searched for for
                    // secondary indexes.
                    // create sql query for hibernate client.
                     f = g.getProperty();
                     collectionHolder.put(relationalValue, childs);
                }
                    onReflect(e.getEntity(), f, (List) collectionHolder.get(relationalValue));
                }
           }
        return e.getEntity();
    }

    /**
     * On reflect.
     *
     * @param entity the entity
     * @param f the f
     * @param childs the childs
     * @return the sets the
     * @throws PropertyAccessException the property access exception
     */
    private Set<?> onReflect(Object entity, Field f, List<?> childs) throws PropertyAccessException
    {
        Set chids = new HashSet();
        if (childs != null)
        {
            chids = new HashSet(childs);
            // TODO: need to store object in sesion.
            // getSession().store(id, entity)
            PropertyAccessorHelper.set(entity, f,
                    PropertyAccessorHelper.isCollection(f.getType()) ? getFieldInstance(childs, f) : childs.get(0));
        }
        return chids;
    }

    /**
     * Gets the field instance.
     * 
     * @param chids
     *            the chids
     * @param f
     *            the f
     * @return the field instance
     */
    private Object getFieldInstance(List chids, Field f)
    {

        if (Set.class.isAssignableFrom(f.getType()))
        {
            Set col = new HashSet(chids);
            return col;
        }
        return chids;
    }

    /**
     * Populate relations.
     *
     * @param relations the relations
     * @param o the o
     * @return the map
     */
    protected Map<String, Object> populateRelations(List<String> relations, Object[] o)
    {
        Map<String, Object> relationVal = new HashMap<String, Object>(relations.size());
        int counter = 1;
        for (String r : relations)
        {
            relationVal.put(r, o[counter++]);
        }
        return relationVal;
    }

    /**
     * Populate entities, in case of there is no relation exist. 
     *
     * @param m the m
     * @param client the client
     * @return the list
     */
    protected abstract List<Object> populateEntities(EntityMetadata m, Client client);

    /**
     * Handle associations.
     *
     * @param m the m
     * @param client the client
     * @param graphs the graphs
     * @param relationNames the relation names
     * @param isParent the is parent
     */
    protected abstract List<Object> handleAssociations(EntityMetadata m, Client client, List<EntitySaveGraph> graphs,
            List<String> relationNames, boolean isParent);
}
