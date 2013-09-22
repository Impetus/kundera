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

import com.impetus.kundera.query.Person;

/**
 * @author vivek.mishra
 * junit for {@link KunderaPersistenceProviderUtil}
 */
public class KunderaPersistenceProviderUtilTest
{

    private KunderaPersistence persistence;
    
    @Before 
    public void setup()
    {
        persistence = new KunderaPersistence();
    }
    
    @Test
    public void testIsLoaded()
    {
        KunderaPersistenceProviderUtil providerUtil = new KunderaPersistenceProviderUtil(persistence);
        LoadState state = providerUtil.isLoaded(null);
        Assert.assertNotNull(state);
        Assert.assertEquals(LoadState.NOT_LOADED, state);
    }

    @Test
    public void testIsLoadedWithOutReference()
    {
        KunderaPersistenceProviderUtil providerUtil = new KunderaPersistenceProviderUtil(persistence);
        Person p = new Person();
        p.setAge(32);
        LoadState state = providerUtil.isLoadedWithoutReference(p,"age");
        Assert.assertNotNull(state);
        Assert.assertEquals(LoadState.LOADED, state);
    }

    @Test
    public void testIsLoadedWithReference()
    {
        KunderaPersistenceProviderUtil providerUtil = new KunderaPersistenceProviderUtil(persistence);
        Person p = new Person();
        p.setAge(32);
        LoadState state = providerUtil.isLoadedWithReference(p,"age");
        Assert.assertNotNull(state);
        Assert.assertEquals(LoadState.UNKNOWN, state);
    }

}
