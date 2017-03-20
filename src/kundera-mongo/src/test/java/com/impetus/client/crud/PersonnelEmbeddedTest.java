/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.crud.entities.PersonalDetailEmbedded;
import com.impetus.client.crud.entities.PersonnelEmbedded;
import com.impetus.client.crud.entities.PhoneDirectory;
import com.impetus.client.utils.MongoUtils;

/**
 * @author Kuldeep.Mishra
 * 
 */
public class PersonnelEmbeddedTest
{
    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("mongoTest");
        em = emf.createEntityManager();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        MongoUtils.dropDatabase(emf, "mongoTest");
        em.close();
        emf.close();
    }

    @Test
    public void test()
    {
        
        List<String> contactName = new ArrayList<String>();
        contactName.add("xamry");
        
        Map<String, String> contactMap =  new HashMap<>();
        contactMap.put("xamry", "9891991919");
        
        Set<String> contactNumber = new HashSet<>();
        contactNumber.add("9891991919");
        
        PhoneDirectory phone = new PhoneDirectory("MyPhoneDirectory",contactName,contactMap,contactNumber);
        
        PersonalDetailEmbedded detail = new PersonalDetailEmbedded();
        detail.setPhoneNo(12456);
        detail.setEmailId("xyz@gmail.com");
        detail.setAddress("BBBBB");
        detail.setPhone(phone);

        PersonnelEmbedded personnel = new PersonnelEmbedded();
        personnel.setId(1);
        personnel.setAge(24);
        personnel.setName("Kuldeep");
        personnel.setPersonalDetail(detail);

        em.persist(personnel);

        em.clear();

        PersonnelEmbedded foundPersonnel = em.find(PersonnelEmbedded.class, 1);
        Assert.assertNotNull(foundPersonnel);
        Assert.assertNotNull(foundPersonnel.getPersonalDetail());
        Assert.assertNotNull(foundPersonnel.getPersonalDetail().getPhone());
        Assert.assertEquals("xamry", foundPersonnel.getPersonalDetail().getPhone().getContactName().get(0));
        Set<String> hashSet = new HashSet<String>();
        hashSet.add("9891991919");
        Assert.assertEquals(hashSet, foundPersonnel.getPersonalDetail().getPhone().getContactNumber());

        List<PersonnelEmbedded> personnels = em.createQuery("Select p from PersonnelEmbedded p").getResultList();
        Assert.assertNotNull(personnels);
        Assert.assertFalse(personnels.isEmpty());
        Assert.assertNotNull(personnels.get(0));
        Assert.assertNotNull(personnels.get(0).getPersonalDetail());
        Assert.assertNotNull(personnels.get(0).getPersonalDetail().getPhone());
        Assert.assertEquals("xamry", personnels.get(0).getPersonalDetail().getPhone().getContactName().get(0));
        Assert.assertEquals(hashSet, personnels.get(0).getPersonalDetail().getPhone().getContactNumber());

        personnels = em.createQuery("Select p from PersonnelEmbedded p where p.age=24").getResultList();
        Assert.assertNotNull(personnels);
        Assert.assertFalse(personnels.isEmpty());
        Assert.assertNotNull(personnels.get(0));
        Assert.assertNotNull(personnels.get(0).getPersonalDetail());
        Assert.assertNotNull(personnels.get(0).getPersonalDetail().getPhone());
        Assert.assertEquals("xamry", personnels.get(0).getPersonalDetail().getPhone().getContactName().get(0));
        Assert.assertEquals(hashSet, personnels.get(0).getPersonalDetail().getPhone().getContactNumber());
        
        personnels = em.createQuery("Select p from PersonnelEmbedded p where p.personalDetail.phoneNo = 12456 and p.age=24").getResultList();
        Assert.assertNotNull(personnels);
        Assert.assertFalse(personnels.isEmpty());
        Assert.assertNotNull(personnels.get(0));
        Assert.assertNotNull(personnels.get(0).getPersonalDetail());
        Assert.assertNotNull(personnels.get(0).getPersonalDetail().getPhone());
        Assert.assertEquals("xamry", personnels.get(0).getPersonalDetail().getPhone().getContactName().get(0));
        Assert.assertEquals(hashSet, personnels.get(0).getPersonalDetail().getPhone().getContactNumber());

        personnels = em.createQuery("Select p from PersonnelEmbedded p where p.personalDetail.phoneNo = 12456 and p.age=2").getResultList();
        Assert.assertTrue(personnels.isEmpty());

        personnels = em.createQuery("Select p from PersonnelEmbedded p where (p.personalDetail.phoneNo = 1245) or (p.age=24)").getResultList();
        Assert.assertNotNull(personnels);
        Assert.assertFalse(personnels.isEmpty());
        Assert.assertNotNull(personnels.get(0));
        Assert.assertNotNull(personnels.get(0).getPersonalDetail());
        Assert.assertNotNull(personnels.get(0).getPersonalDetail().getPhone());
        Assert.assertEquals("xamry", personnels.get(0).getPersonalDetail().getPhone().getContactName().get(0));
        Assert.assertEquals(hashSet, personnels.get(0).getPersonalDetail().getPhone().getContactNumber());
        
        personnels = em.createQuery("Select p from PersonnelEmbedded p where p.personalDetail.emailId like :email").setParameter("email" , "xyz%").getResultList();
        Assert.assertNotNull(personnels);
        Assert.assertFalse(personnels.isEmpty());
        Assert.assertNotNull(personnels.get(0));
        Assert.assertNotNull(personnels.get(0).getPersonalDetail());
        Assert.assertNotNull(personnels.get(0).getPersonalDetail().getPhone());
        Assert.assertEquals("xamry", personnels.get(0).getPersonalDetail().getPhone().getContactName().get(0));
        Assert.assertEquals(hashSet, personnels.get(0).getPersonalDetail().getPhone().getContactNumber());
        
        personnels = em.createQuery("Select p from PersonnelEmbedded p where p.personalDetail.emailId like :email").setParameter("email" , "xyz_%").getResultList();
        Assert.assertNotNull(personnels);
        Assert.assertFalse(personnels.isEmpty());
        Assert.assertNotNull(personnels.get(0));
        Assert.assertEquals(1, personnels.size());
        
        personnels = em.createQuery("Select p from PersonnelEmbedded p where p.personalDetail.emailId like :email").setParameter("email" , "xyz_%").getResultList();
        Assert.assertNotNull(personnels);
        Assert.assertFalse(personnels.isEmpty());
        Assert.assertEquals(1, personnels.size());
        
        personnels = em.createQuery("Select p from PersonnelEmbedded p where p.personalDetail.emailId like :email").setParameter("email" , "xyz").getResultList();
        Assert.assertNotNull(personnels);
        Assert.assertTrue(personnels.isEmpty());
        Assert.assertEquals(0, personnels.size());
       

    }

    @Test
    public void testAggregation()
    {
        PersonalDetailEmbedded embedded1 = new PersonalDetailEmbedded();
        embedded1.setAddress("Address 1");
        embedded1.setEmailId("email1@company.com");
        embedded1.setPhoneNo(90001);
        embedded1.setPhone(new PhoneDirectory("dir1",
              Collections.singletonList("contact1"), Collections.singletonMap("contact1", "90091"),
              Collections.singleton("90091")));

        PersonnelEmbedded personnel1 = new PersonnelEmbedded();
        personnel1.setId(1);
        personnel1.setAge(25);
        personnel1.setName("Person 1");
        personnel1.setPersonalDetail(embedded1);

        em.persist(personnel1);

        PersonalDetailEmbedded embedded2 = new PersonalDetailEmbedded();
        embedded2.setAddress("Address 2");
        embedded2.setEmailId("email2@company.com");
        embedded2.setPhoneNo(90002);
        embedded2.setPhone(new PhoneDirectory("dir2",
              Collections.singletonList("contact2"), Collections.singletonMap("contact2", "90092"),
              Collections.singleton("90092")));

        PersonnelEmbedded personnel2 = new PersonnelEmbedded();
        personnel2.setId(2);
        personnel2.setAge(32);
        personnel2.setName("Person 2");
        personnel2.setPersonalDetail(embedded2);

        em.persist(personnel2);

        Query query = em.createQuery("select max(p.id) from PersonnelEmbedded p");
        Object result = query.getSingleResult();

        Assert.assertNotNull(result);
        Assert.assertEquals(2, result);

        query = em.createQuery("select min(p.id) from PersonnelEmbedded p");
        result = query.getSingleResult();

        Assert.assertNotNull(result);
        Assert.assertEquals(1, result);

        query = em.createQuery("select avg(p.id) from PersonnelEmbedded p");
        result = query.getSingleResult();

        Assert.assertNotNull(result);
        Assert.assertEquals(1.5, result);

        query = em.createQuery("select sum(p.id) from PersonnelEmbedded p");
        result = query.getSingleResult();

        Assert.assertNotNull(result);
        Assert.assertEquals(3, result);
    }

}