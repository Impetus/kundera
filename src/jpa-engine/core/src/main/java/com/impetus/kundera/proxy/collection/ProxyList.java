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
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.PersistenceDelegator;

/**
 * Proxy class used to represent instances for {@link List}
 * 
 * @author amresh.singh
 */
public class ProxyList extends AbstractProxyCollection implements ProxyCollection, List
{

    /**
     * Default constructor
     */
    public ProxyList()
    {
        super();
    }

    public ProxyList(final PersistenceDelegator delegator, final Relation relation)
    {
        super(delegator, relation);
    }

    @Override
    public ProxyCollection getCopy()
    {
        ProxyCollection proxyCollection = new ProxyList(getPersistenceDelegator(), getRelation());
        proxyCollection.setRelationsMap(getRelationsMap());
        return proxyCollection;
    }

    // ///////////Methods from Collection interface////////////////////////////
    @Override
    public boolean contains(final Object arg0)
    {
        return super.contains(arg0);
    }

    @Override
    public boolean containsAll(final Collection arg0)
    {
        return super.containsAll(arg0);
    }

    @Override
    public void clear()
    {
        super.clear();

    }

    @Override
    public boolean isEmpty()
    {
        return super.isEmpty();
    }

    @Override
    public boolean add(final Object arg0)
    {
        return super.add(arg0);
    }

    @Override
    public boolean addAll(final Collection arg0)
    {
        return super.addAll(arg0);
    }

    @Override
    public boolean remove(final Object arg0)
    {
        return super.remove(arg0);
    }

    @Override
    public boolean removeAll(final Collection collection)
    {
        return super.removeAll(collection);
    }

    @Override
    public boolean retainAll(final Collection collection)
    {
        return super.retainAll(collection);
    }

    @Override
    public Iterator iterator()
    {
        return super.iterator();
    }

    @Override
    public Object[] toArray()
    {
        return super.toArray();
    }

    @Override
    public Object[] toArray(final Object[] arg0)
    {
        return super.toArray(arg0);
    }

    @Override
    public int size()
    {
        return super.size();
    }

    // ////////////Methods from List interface ///////////////////////

    @Override
    public void add(final int arg0, final Object arg1)
    {
        eagerlyLoadDataCollection();

        List dataList = (List) dataCollection;
        if(dataList == null)
        {
            dataList = new ArrayList();
        }

        if (arg1 != null && !dataList.contains(arg1))
        {            
            dataList.add(arg0, arg1);
        }
    }

    @Override
    public boolean addAll(final int index, final Collection collection)
    {
        eagerlyLoadDataCollection();

        boolean result = false;

        List dataList = (List) dataCollection;
        if(dataList == null)
        {
            dataList = new ArrayList();
        }
        
        if (collection != null && !collection.isEmpty())
        {
            int position = 0;
            for (Object o : collection)
            {
                if (o != null && !dataList.contains(o))
                {                    
                    dataList.add(index + position++, o);
                }
            }
            result = true;
        }
        return result;
    }

    @Override
    public Object get(final int arg0)
    {
        eagerlyLoadDataCollection();

        Object result = null;

        List dataList = (List) dataCollection;
        if (dataList != null && !dataList.isEmpty())
        {
            result = dataList.get(arg0);
        }
        return result;
    }

    @Override
    public int indexOf(final Object arg0)
    {
        eagerlyLoadDataCollection();

        int index = -1;
        List dataList = (List) dataCollection;
        if (dataList != null && !dataList.isEmpty())
        {
            index = dataList.indexOf(arg0);
        }
        return index;
    }

    @Override
    public int lastIndexOf(final Object arg0)
    {
        eagerlyLoadDataCollection();

        int index = -1;

        List dataList = (List) dataCollection;
        if (dataList != null && !dataList.isEmpty())
        {
            index = dataList.lastIndexOf(arg0);
        }
        return index;
    }

    @Override
    public ListIterator listIterator()
    {
        eagerlyLoadDataCollection();

        ListIterator iterator = null;

        List dataList = (List) dataCollection;
        if (dataList != null && !dataList.isEmpty())
        {
            iterator = dataList.listIterator();
        }
        return iterator;
    }

    @Override
    public ListIterator listIterator(final int arg0)
    {
        eagerlyLoadDataCollection();

        ListIterator iterator = null;

        List dataList = (List) dataCollection;
        if (dataList != null && !dataList.isEmpty())
        {
            iterator = dataList.listIterator(arg0);
        }
        return iterator;
    }

    @Override
    public Object remove(final int arg0)
    {
        eagerlyLoadDataCollection();

        Object result = null;

        List dataList = (List) dataCollection;
                
        if (dataList != null && !dataList.isEmpty() && dataList.contains(arg0))
        {            
            result = dataList.remove(arg0);
        }
        return result;
    }

    @Override
    public Object set(final int arg0, final Object arg1)
    {
        eagerlyLoadDataCollection();
        List dataList = (List) dataCollection;

        Object result = null;

        if (dataList != null && !dataList.isEmpty())
        {
            if (dataList.get(arg0) == null)
            {
                getPersistenceDelegator().persist(arg1);
                result = dataList.set(arg0, arg1);

            }
            else
            {
                getPersistenceDelegator().merge(arg1);
                result = dataList.set(arg0, arg1);
            }

        }
        return result;
    }

    @Override
    public List subList(final int arg0, final int arg1)
    {
        eagerlyLoadDataCollection();

        List result = null;

        List dataList = (List) dataCollection;
        if (dataList != null && !dataList.isEmpty())
        {
            result = dataList.subList(arg0, arg1);
        }
        return result;
    }

}
