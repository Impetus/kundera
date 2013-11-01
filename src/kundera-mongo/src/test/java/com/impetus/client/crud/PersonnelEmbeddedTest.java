/**
 * 
 */
package com.impetus.client.crud;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author impadmin
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
        em.close();
        emf.close();
    }

    @Test
    public void test()
    {
        PhoneDirectory phone = new PhoneDirectory();

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
    }
}