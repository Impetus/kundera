/*******************************************************************************
 *  * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.proxy.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * Abstract class containing common methods for all interfaces extending
 * {@link Collection} interface
 * 
 * @author amresh.singh
 */
public abstract class AbstractProxyCollection extends AbstractProxyBase
{

    /**
     * Default constructor
     */
    public AbstractProxyCollection()
    {
        super();
    }

    /**
     * @param delegator
     */
    public AbstractProxyCollection(final PersistenceDelegator delegator, final Relation relation)
    {
        super(delegator, relation);
    }

    @Override
    public Object getDataCollection()
    {
        return dataCollection != null /*&& ! ((Collection) dataCollection).isEmpty() */? dataCollection : null;
    }
    
    protected boolean add(final Object object)
    {
        eagerlyLoadDataCollection();

        boolean result = false;

        if (dataCollection == null)
        {
            createEmptyDataCollection();
        }

        if (dataCollection != null && !(dataCollection instanceof ProxyCollection) && ! ((Collection) dataCollection).contains(object)
                && object != null)
        {
            // getPersistenceDelegator().persist(object);
            ((Collection) dataCollection).add(object);
            PropertyAccessorHelper.set(getOwner(), getRelation().getProperty(), dataCollection);
            result = true;
        }
        return result;
    }   
    
    protected void clear()
    {
        eagerlyLoadDataCollection();
        if (getDataCollection() != null && !(getDataCollection() instanceof ProxyCollection))
        {
            ((Collection)getDataCollection()).clear();
        }
    }
    
    protected boolean contains(Object arg0)
    {

        boolean result = false;

        eagerlyLoadDataCollection();
        if (getDataCollection() != null && !(getDataCollection() instanceof ProxyCollection))
        {
            result = ((Collection)getDataCollection()).contains(arg0);
        }
        return result;
    }
    
    protected boolean isEmpty()
    {
        boolean result = true;

        eagerlyLoadDataCollection();
        if (getDataCollection() != null && !(getDataCollection() instanceof ProxyCollection))
        {
            result = ((Collection)getDataCollection()).isEmpty();
        }
        return result;
    }
    
    protected int size()
    {
        eagerlyLoadDataCollection();
        return dataCollection == null || dataCollection instanceof ProxyCollection ? 0 : ((Collection) dataCollection).size();
    }

    protected boolean addAll(final Collection collection)
    {
        eagerlyLoadDataCollection();

        boolean result = false;
        
        if (dataCollection == null)
        {
            createEmptyDataCollection();
        }

        if (dataCollection != null && !(dataCollection instanceof ProxyCollection) && collection != null
                && !collection.isEmpty())
        {            
            ((Collection) dataCollection).addAll(collection);
            result = true;
        }
        return result;
    }

    protected boolean remove(final Object object)
    {
        eagerlyLoadDataCollection();

        boolean result = false;
        
        if (dataCollection == null)
        {
            createEmptyDataCollection();
        }
        
        if (dataCollection != null && !(dataCollection instanceof ProxyCollection) && object != null)
        {
            
            ((Collection) dataCollection).remove(object);            
            result = true;
        }
        return result;
    }

    protected boolean removeAll(final Collection collection)
    {
        eagerlyLoadDataCollection();
        boolean result = false;
        
        if (dataCollection == null)
        {
            createEmptyDataCollection();
        }
        
        if (dataCollection != null && !(dataCollection instanceof ProxyCollection) && collection != null
                && !collection.isEmpty())
        {
            ((Collection) dataCollection).removeAll(collection);           
            result = true;
        }
        return result;
    }
    
    protected boolean containsAll(final Collection arg0)
    {
        eagerlyLoadDataCollection();

        boolean result = false;
        
        if(dataCollection == null)
        {
            createEmptyDataCollection();
        }

        if (getDataCollection() != null && !(dataCollection instanceof ProxyCollection) && arg0 != null && ! arg0.isEmpty())
        {
            result = ((Collection)getDataCollection()).containsAll(arg0);
        }
        return result;
    }

    protected boolean retainAll(final Collection collection)
    {
        boolean result = false;
        eagerlyLoadDataCollection();

        if (dataCollection == null)
        {
            createEmptyDataCollection();
        }
        
        if (dataCollection != null && !(dataCollection instanceof ProxyCollection) && collection != null
                && !collection.isEmpty())
        {
            result = ((Collection) dataCollection).retainAll(collection);
        }

        return result;
    }

    protected Iterator iterator()
    {

        eagerlyLoadDataCollection();
        Iterator result = null;
        if (getDataCollection() != null && !(getDataCollection() instanceof ProxyCollection))
        {
            result = ((Collection) getDataCollection()).iterator();
        }
        return result;
    }

    protected Object[] toArray()
    {
        eagerlyLoadDataCollection();
        return dataCollection == null ? new Object[0] : ((Collection) dataCollection).toArray();
    }

    protected Object[] toArray(final Object[] arg0)
    {
        eagerlyLoadDataCollection();
        return dataCollection == null ? new Object[0] : ((Collection) dataCollection).toArray(arg0);
    }
    
    /**
     * Creates an a data collection which has no entity inside it
     */
    private void createEmptyDataCollection()
    {
        Class<?> collectionClass = getRelation().getProperty().getType();
        if (collectionClass.isAssignableFrom(Set.class))
        {
            dataCollection = new HashSet();
        }
        else if (collectionClass.isAssignableFrom(List.class))
        {
            dataCollection = new ArrayList();
        }
    }
}
