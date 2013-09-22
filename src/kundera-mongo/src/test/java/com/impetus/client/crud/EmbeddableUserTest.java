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
package com.impetus.client.crud;

import javax.persistence.Embeddable;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.model.KunderaMetadata;

/**
 * EmbeddableUserTest tests @{@link Entity} and {@link Embeddable} having
 * {@link Set, List, Map} as its attribute.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public class EmbeddableUserTest
{

    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    @Before
    public void setUp()
    {
        emf = Persistence.createEntityManagerFactory("mongoTest");
        em = emf.createEntityManager();
    }

    @Test
    public void test()
    {
        AppUser user = new AppUser();
        user.setId("id");
        PhoneDirectory properties = new PhoneDirectory();
        user.setPropertyContainer(properties);
        em.persist(user);

        em.clear();

        AppUser result = em.find(AppUser.class, "id");

        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getId());
        Assert.assertNotNull(result.getPropertyKeys());
        Assert.assertFalse(result.getPropertyKeys().isEmpty());
        Assert.assertEquals(1, result.getPropertyKeys().size());
        Assert.assertNotNull(result.getNickName());
        Assert.assertFalse(result.getNickName().isEmpty());
        Assert.assertEquals(1, result.getNickName().size());
        Assert.assertTrue(result.getNickName().contains("kk"));
        Assert.assertNotNull(result.getFriendList());
        Assert.assertFalse(result.getFriendList().isEmpty());
        Assert.assertEquals(2, result.getFriendList().size());
        Assert.assertNotNull(result.getTags());
        Assert.assertFalse(result.getTags().isEmpty());
        Assert.assertEquals(1, result.getTags().size());
        Assert.assertEquals("yo", result.getTags().get(0));

        PhoneDirectory propertyContainer = result.getPhoneDirectory();
        Assert.assertNotNull(propertyContainer);
        Assert.assertEquals("MyPhoneDirectory", propertyContainer.getPhoneDirectoryName());
        Assert.assertNotNull(propertyContainer.getContactMap());
        Assert.assertFalse(propertyContainer.getContactMap().isEmpty());
        Assert.assertEquals(1, propertyContainer.getContactMap().size());
        Assert.assertEquals("9891991919", propertyContainer.getContactMap().get("xamry"));
        Assert.assertNotNull(propertyContainer.getContactNumber());
        Assert.assertFalse(propertyContainer.getContactNumber().isEmpty());
        Assert.assertEquals(1, propertyContainer.getContactNumber().size());
        Assert.assertNotNull(propertyContainer.getContactName());
        Assert.assertFalse(propertyContainer.getContactName().isEmpty());
        Assert.assertEquals(1, propertyContainer.getContactName().size());
        Assert.assertEquals("xamry", propertyContainer.getContactName().get(0));

    }

    @After
    public void tearDown()
    {
        em.close();
        emf.close();
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
    }
}
