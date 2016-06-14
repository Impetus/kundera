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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Embeddable;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.entities.AppUser;
import com.impetus.client.crud.entities.PhoneDirectory;

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
        List<String> contactList = new ArrayList<>();
        contactList.add("xamry");

        Map<String, String> contactMap = new HashMap<>();
        contactMap.put("xamry", "9891991919");

        Set<String> contactSet = new HashSet<>();
        contactSet.add("xamry");

        PhoneDirectory properties = new PhoneDirectory("MyPhoneDirectory", contactList, contactMap, contactSet);
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

    /**
     * Tests validation constraint on embeddable object
     */
    @Test
    public void testConstraints()
    {
        try
        {
            AppUser user = new AppUser();
            user.setId("id");
            List<String> contactName = new LinkedList<String>();
            Map<String, String> contactMap = new HashMap<String, String>();
            Set<String> contactNumber = new HashSet<String>();
            contactName.add("xamry");
            contactMap.put("xamry", "9891991919");
            contactMap.put("xamry1", "98919919129");
            contactMap.put("xamry2", "98919919319");
            contactNumber.add("9891991919");
            String phoneDirectoryName = "MyPhoneDirectory";
            PhoneDirectory properties = new PhoneDirectory(phoneDirectoryName, contactName, contactMap, contactNumber);
            user.setPropertyContainer(properties);
            em.persist(user);

            em.clear();

        }
        catch (Exception e)
        {
            Assert.fail();
            Assert.assertEquals(
                    "javax.validation.ValidationException: The size should be at least equal to one but not more than 2",
                    e.getMessage());

        }

    }

    @After
    public void tearDown()
    {
        em.close();
        emf.close();

    }
}
