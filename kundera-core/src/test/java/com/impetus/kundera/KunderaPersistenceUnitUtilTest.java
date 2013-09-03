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

import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnitUtil;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.query.Person;

/**
 * @author vivek.mishra
 * junit for {@link KunderaPersistenceUnitUtil}
 */
public class KunderaPersistenceUnitUtilTest
{
    private EntityManagerFactory emf;
    
    @Before
    public void setup()
    {
        KunderaPersistence persistence = new KunderaPersistence();
        emf = persistence.createEntityManagerFactory("patest", null);

    }

    @Test
    public void testIsLoaded()
    {
        PersistenceUnitUtil utils = emf.getPersistenceUnitUtil();
        
        Assert.assertNotNull(utils);
        Assert.assertFalse(utils.isLoaded(null));
    }
    
    @Test
    public void testIsLoadedWithoutReference()
    {
        PersistenceUnitUtil utils = emf.getPersistenceUnitUtil();
        
        Person p = new Person();
        p.setAge(32);
        p.setPersonId("1");
      
        Assert.assertNotNull(utils);
        Assert.assertTrue(utils.isLoaded(p, "personId"));
        Assert.assertFalse(utils.isLoaded(null, "personName"));
    }

    @Test
    public void testGetIdentifier()
    {
        PersistenceUnitUtil utils = emf.getPersistenceUnitUtil();
        
        Person p = new Person();
        p.setAge(32);
        p.setPersonId("1");
        
        Assert.assertNotNull(utils.getIdentifier(p));
        Assert.assertEquals("1",utils.getIdentifier(p));
    }

    @Test
    public void testInvalidEntity()
    {
        PersistenceUnitUtil utils = emf.getPersistenceUnitUtil();
        
        Person p = new Person();
        p.setAge(32);
        p.setPersonId("1");
        
        try
        {
            utils.getIdentifier(new String("test"));
            Assert.fail("Should have gone to catch block!");
        } catch(IllegalArgumentException iaex)
        {
            Assert.assertEquals(String.class + " is not an entity", iaex.getMessage());
        }
    }

}
