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
package com.impetus.kundera.persistence;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Junit to test load persistence unit specfic properties via external map.
 * 
 * @author shaheed.hussain
 * 
 */
public class ExternalPropertyLoaderTest
{

    private EntityManagerFactory emf;

    @Before
    public void setUpBefore() throws Exception
    {
        Map propertyMap = new HashMap();
        propertyMap.put("kundera.nodes", "localhost");
        propertyMap.put("kundera.port", "9160");
        propertyMap.put("kundera.keyspace", "sprint");
        propertyMap.put("kundera.client.lookup.class", "com.impetus.kundera.client.CoreTestClientFactory");
        emf = Persistence.createEntityManagerFactory("extConfig", propertyMap);
    }

    @After
    public void tearDown() throws Exception
    {
        emf.close();
    }

    /**
     * Junit to verify, if emf has been successfully loaded and user object has
     * been persisted.
     */
    @Test
    public void test()
    {
        EntityManager em = emf.createEntityManager();
        User user = new User();
        user.setFirstName("John");
        user.setLastName("Smith");
        user.setCity("London");
        em.persist(user);

        em.clear();

        User found = em.find(User.class, user.getUserId()); // it is auto generated in-memory
                                              // id
        Assert.assertNotNull(found);
        Assert.assertEquals(user.getCity(), found.getCity());
        em.close();
    }

}
