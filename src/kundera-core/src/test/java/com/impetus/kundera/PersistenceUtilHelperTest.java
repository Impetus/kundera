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
package com.impetus.kundera;

import javax.persistence.spi.LoadState;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.PersistenceUtilHelper.MetadataCache;
import com.impetus.kundera.proxy.KunderaProxy;
import com.impetus.kundera.proxy.cglib.CglibLazyInitializerFactory;
import com.impetus.kundera.query.Person;

/**
 * @author vivek.mishra 
 * junit for {@link PersistenceUtilHelper}.
 * 
 */
public class PersistenceUtilHelperTest
{

    private MetadataCache cache;

    @Before
    public void setUp()
    {
        this.cache = new PersistenceUtilHelper.MetadataCache();
    }

    @Test
    public void testisLoadedWithReferenceAsNull()
    {
        LoadState state = PersistenceUtilHelper.isLoadedWithReference(null, null, cache);

        Assert.assertNotNull(state);
        Assert.assertEquals(LoadState.NOT_LOADED, state);
    }

    @Test
    public void testisLoadedWithReferenceAsLoaded()
    {
        Person p = new Person();
        p.setPersonName("Vivek");
        LoadState state = PersistenceUtilHelper.isLoadedWithReference(p, "personName", cache);

        Assert.assertNotNull(state);
        Assert.assertEquals(LoadState.UNKNOWN, state);
    }

    @Test
    public void testisLoadedWithReferenceAsKunderaProxy() throws NoSuchMethodException, SecurityException
    {
        CglibLazyInitializerFactory factory = new CglibLazyInitializerFactory();
        KunderaProxy proxy = factory.getProxy("Person", Person.class,
                Person.class.getDeclaredMethod("getPersonId", null),
                Person.class.getDeclaredMethod("setPersonId", String.class), "personId", null);

        LoadState state = PersistenceUtilHelper.isLoadedWithReference(proxy, "personId", cache);

        Assert.assertNotNull(state);
        Assert.assertEquals(LoadState.UNKNOWN, state);
    }

    @Test
    public void testisLoadedWithoutReferenceAsNull()
    {
        LoadState state = PersistenceUtilHelper.isLoadedWithoutReference(null, null, cache);

        Assert.assertNotNull(state);
        Assert.assertEquals(LoadState.NOT_LOADED, state);
    }

    @Test
    public void testisLoadedWithoutReferenceAsLoad()
    {
        Person p = new Person();
        p.setPersonName("Vivek");

        LoadState state = PersistenceUtilHelper.isLoadedWithoutReference(p, "personName", cache);

        Assert.assertNotNull(state);
        Assert.assertEquals(LoadState.LOADED, state);
    }

    @Test
    public void testisLoadedWithOutReferenceAsKunderaProxy() throws NoSuchMethodException, SecurityException
    {
        CglibLazyInitializerFactory factory = new CglibLazyInitializerFactory();
        KunderaProxy proxy = factory.getProxy("Person", Person.class,
                Person.class.getDeclaredMethod("getPersonId", null),
                Person.class.getDeclaredMethod("setPersonId", String.class), "personId", null);

        LoadState state = PersistenceUtilHelper.isLoadedWithoutReference(proxy, "personName", cache);

        Assert.assertNotNull(state);
        Assert.assertEquals(LoadState.NOT_LOADED, state);
        
//        factory.
    }
    
    @Test
    public void testisLoadedWAsKunderaProxy() throws NoSuchMethodException, SecurityException
    {
        CglibLazyInitializerFactory factory = new CglibLazyInitializerFactory();
        KunderaProxy proxy = factory.getProxy("Person", Person.class,
                Person.class.getDeclaredMethod("getPersonId", null),
                Person.class.getDeclaredMethod("setPersonId", String.class), "personId", null);

        LoadState state = PersistenceUtilHelper.isLoaded(proxy);

        Assert.assertNotNull(state);
        Assert.assertEquals(LoadState.NOT_LOADED, state);
        
//        factory.
    }

}
