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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.persistence.PersistenceDelegator;

/**
 * Proxy class used to represent instances for {@link Map}
 * 
 * @author amresh.singh
 */
public class ProxyMap extends AbstractProxyBase implements ProxyCollection, Map
{

    /**
     * Default constructor
     */
    public ProxyMap()
    {
        super();
    }

    public ProxyMap(final PersistenceDelegator delegator, final Relation relation)
    {
        super(delegator, relation);
    }

    @Override
    public ProxyCollection getCopy()
    {
        ProxyCollection proxyCollection = new ProxyMap(getPersistenceDelegator(), getRelation());
        proxyCollection.setRelationsMap(getRelationsMap());
        return proxyCollection;
    }
    
    @Override
    public Object getDataCollection()
    {
        return dataCollection != null && ! ((Map) dataCollection).isEmpty() ? dataCollection : null;
    }

    // ///////////////////////Methods from Collection interface ////////////////

    @Override
    public void clear()
    {
        eagerlyLoadDataCollection();
        if (getDataCollection() != null && !(getDataCollection() instanceof ProxyCollection))
        {
            ((Map)getDataCollection()).clear();
        }
    }

    @Override
    public boolean isEmpty()
    {
        boolean result = true;

        eagerlyLoadDataCollection();
        if (getDataCollection() != null && !(getDataCollection() instanceof ProxyCollection))
        {
            result = ((Map)getDataCollection()).isEmpty();
        }
        return result;
    }

    @Override
    public int size()
    {
        eagerlyLoadDataCollection();
        return dataCollection == null || dataCollection instanceof ProxyCollection ? 0 : ((Map) dataCollection).size();
    }

    // ///////////////////////Methods from Map interface ////////////////

    @Override
    public boolean containsKey(final Object arg0)
    {
        eagerlyLoadDataCollection();
        final Map dataMap = (Map) dataCollection;

        boolean result = false;

        if (dataMap != null && !(dataMap instanceof ProxyMap) && !dataMap.isEmpty())
        {
            result = dataMap.containsKey(arg0);
        }
        return result;
    }

    @Override
    public boolean containsValue(final Object arg0)
    {
        eagerlyLoadDataCollection();
        final Map dataMap = (Map) dataCollection;

        boolean result = false;

        if (dataMap != null && !dataMap.isEmpty())
        {
            result = dataMap.containsValue(arg0);
        }
        return result;
    }

    @Override
    public Set entrySet()
    {
        eagerlyLoadDataCollection();
        final Map dataMap = (Map) dataCollection;

        Set result = null;

        if (dataMap != null && !dataMap.isEmpty())
        {
            result = dataMap.entrySet();
        }
        return result;
    }

    @Override
    public Object get(final Object arg0)
    {
        eagerlyLoadDataCollection();
        final Map dataMap = (Map) dataCollection;

        Object result = null;

        if (dataMap != null && !dataMap.isEmpty())
        {
            result = dataMap.get(arg0);
        }
        return result;
    }

    @Override
    public Set keySet()
    {
        eagerlyLoadDataCollection();
        final Map dataMap = (Map) dataCollection;

        Set result = null;

        if (dataMap != null && !dataMap.isEmpty())
        {
            result = dataMap.keySet();
        }
        return result;
    }

    @Override
    public Object put(final Object arg0, final Object arg1)
    {
        eagerlyLoadDataCollection();
        Map dataMap = (Map) dataCollection;

        Object result = null;

        if (dataMap != null)
        {
            result = dataMap.put(arg0, arg1);
        }
        return result;
    }

    @Override
    public void putAll(final Map arg0)
    {
        eagerlyLoadDataCollection();
        Map dataMap = (Map) dataCollection;

        if (dataMap != null)
        {
            dataMap.putAll(arg0);
        }

    }

    @Override
    public Object remove(final Object arg0)
    {
        eagerlyLoadDataCollection();
        Map dataMap = (Map) dataCollection;

        Object result = null;

        if (dataMap != null && !dataMap.isEmpty())
        {
            result = dataMap.remove(arg0);
        }
        return result;
    }

    @Override
    public Collection values()
    {
        eagerlyLoadDataCollection();
        final Map dataMap = (Map) dataCollection;

        Collection result = null;

        if (dataMap != null && !dataMap.isEmpty())
        {
            result = dataMap.values();
        }
        return result;
    } 

}
