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
package com.impetus.kundera.proxy;

import java.util.ArrayList;
import java.util.List;

import javassist.util.proxy.ProxyFactory;

import com.impetus.kundera.proxy.collection.ProxyCollection;

/**
 * Utility class for proxy objects
 * 
 * @author amresh.singh
 */
public class ProxyHelper
{

    static List<String> proxyTypes = new ArrayList<String>();

    static List<String> persistentCollectionTypes = new ArrayList<String>();

    static
    {
        proxyTypes.add("org.hibernate.proxy.HibernateProxy");
        proxyTypes.add("org.hibernate.proxy.map.MapProxy");
        proxyTypes.add("org.hibernate.proxy.dom4j.Dom4jProxy");

        persistentCollectionTypes.add("org.hibernate.collection.spi.PersistentCollection");
        persistentCollectionTypes.add("org.hibernate.collection.internal.AbstractPersistentCollection");
        persistentCollectionTypes.add("org.hibernate.collection.internal.PersistentBag");
        persistentCollectionTypes.add("org.hibernate.collection.internal.PersistentList");
        persistentCollectionTypes.add("org.hibernate.collection.internal.PersistentSet");
        persistentCollectionTypes.add("org.hibernate.collection.internal.PersistentSortedSet");
        persistentCollectionTypes.add("org.hibernate.collection.internal.PersistentMap");
        persistentCollectionTypes.add("org.hibernate.collection.internal.PersistentSortedMap");
    }

    public static boolean isKunderaProxy(Object o)
    {
        return o == null ? false : o instanceof KunderaProxy;
    }

    public static boolean isHibernateProxy(Object o)
    {
        return o == null ? false : proxyTypes.contains(o.getClass().getName());
    }

    public static boolean isKunderaProxyCollection(Object collection)
    {
        return collection == null ? false : collection instanceof ProxyCollection;
    }

    public static boolean isPersistentCollection(Object collection)
    {
        return collection == null ? false : persistentCollectionTypes.contains(collection.getClass().getName());
    }

    public static boolean isProxy(Object o)
    {
        return isKunderaProxy(o) || isHibernateProxy(o) || ProxyFactory.isProxyClass(o.getClass());
    }

    public static boolean isProxyCollection(Object o)
    {
        return isKunderaProxyCollection(o) || isPersistentCollection(o);
    }

    public static boolean isProxyOrCollection(Object o)
    {
        return isProxy(o) || isProxyCollection(o);
    }
}
