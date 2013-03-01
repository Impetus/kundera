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
package com.impetus.kundera.persistence;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.cache.ehcache.CoreTestClient;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.configure.CoreEntityAddressUni1To1;
import com.impetus.kundera.entity.PersonnelDTO;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;

public class PersistenceDelegatorTest
{
    private static EntityManagerFactory emf;

    private static EntityManager em;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
    	KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        emf = Persistence.createEntityManagerFactory("kunderatest");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        em.close();
        emf.close();
    }

    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
    }

    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testPersist()
    {
        PersonnelDTO dto = new PersonnelDTO();
        try
        {
            em.persist(dto);
        }
        catch (KunderaException e)
        {
            Assert.assertEquals(
                    "java.lang.IllegalArgumentException: Entity object is invalid, operation failed. Please check previous log message for details",
                    e.getMessage());
        }
        try
        {
            CoreEntityAddressUni1To1 Oneto1 = new CoreEntityAddressUni1To1();
            em.persist(Oneto1);
        }
        catch (KunderaException e)
        {
            Assert.assertEquals(
                    "com.impetus.kundera.KunderaException: Unable to load entity metadata for :class com.impetus.kundera.configure.CoreEntityAddressUni1To1",
                    e.getMessage());
        }

        em.clear();

        dto = new PersonnelDTO();
        dto.setPersonId("123");
        em.persist(dto);
        dto = em.find(PersonnelDTO.class, "123");
        Assert.assertNotNull(dto);
        Assert.assertEquals("123", dto.getPersonId());
    }

    @Test
    public void testFindById()
    {
        PersonnelDTO dto = new PersonnelDTO();
        dto.setPersonId("123");
        em.persist(dto);
        try
        {
            em.find(null, null);
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertEquals("PrimaryKey value must not be null for object you want to find.", e.getMessage());
        }
        try
        {
            em.find(null, 123);
        }
        catch (KunderaException e)
        {
            Assert.assertEquals("Invalid class provided " + null, e.getMessage());
        }

        try
        {
            em.find(PersonnelDTO.class, null);
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertEquals("PrimaryKey value must not be null for object you want to find.", e.getMessage());
        }

        try
        {
            em.find(PersonnelDTO.class, null);
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertEquals("PrimaryKey value must not be null for object you want to find.", e.getMessage());
        }
        dto = em.find(PersonnelDTO.class, "123");
        Assert.assertNotNull(dto);
    }

    @Test
    public void testRemove()
    {
        PersonnelDTO paramObject = new PersonnelDTO();
        PersonnelDTO dto = new PersonnelDTO();
        dto.setPersonId("123");
        em.persist(dto);
        dto = em.find(PersonnelDTO.class, 123);
        try
        {
            em.remove(null);
        }
        catch (KunderaException e)
        {
            Assert.assertEquals("java.lang.IllegalArgumentException: Entity to be removed must not be null.",
                    e.getMessage());
        }

        em.remove(dto);

        dto = em.find(PersonnelDTO.class, "123");
        Assert.assertNull(dto);
        try
        {
            em.remove(paramObject);
        }
        catch (Exception e)
        {
            Assert.assertEquals("123", "123");
        }

    }

    @Test
    public void testMerge()
    {
        PersonnelDTO dto = new PersonnelDTO();
        dto.setPersonId("123");
        em.persist(dto);
        dto = em.find(PersonnelDTO.class, 123);
        Assert.assertNotNull(dto);

        dto.setFirstName("kuldeep");
        em.merge(dto);
        dto = em.find(PersonnelDTO.class, 123);
        Assert.assertNotNull(dto);
        Assert.assertEquals("kuldeep", dto.getFirstName());
        try
        {
            em.merge(null);
        }
        catch (KunderaException e)
        {
            Assert.assertEquals("java.lang.IllegalArgumentException: Entity to be merged must not be null.",
                    e.getMessage());
        }

        em.merge(new PersonnelDTO());
    }

    @Test
    public void testDetach()
    {
        PersonnelDTO dto = new PersonnelDTO();
        PersistenceDelegator pd = ((EntityManagerImpl) em).getPersistenceDelegator();
        try
        {
            pd.detach(dto);
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertEquals("Primary key not set into entity", e.getMessage());
        }

        dto.setPersonId("123");
        try
        {
            pd.detach(dto);
        }
        catch (IllegalArgumentException e)
        {
            Assert.fail();
        }

        Assert.assertFalse(pd.contains(dto));

        em.persist(dto);

        pd.detach(dto);

        Assert.assertFalse(pd.contains(dto));
    }

    @Test
    public void testGetClient()
    {
        PersonnelDTO dto = new PersonnelDTO();
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(dto.getClass());
        PersistenceDelegator pd = ((EntityManagerImpl) em).getPersistenceDelegator();
        Client c = pd.getClient(entityMetadata);
        Assert.assertNotNull(c);
        Assert.assertTrue(c instanceof CoreTestClient);

    }

    @Test
    public void testIsOpen()
    {
        PersistenceDelegator pd = ((EntityManagerImpl) em).getPersistenceDelegator();
        Assert.assertTrue(pd.isOpen());

        pd.close();
        Assert.assertFalse(pd.isOpen());
    }

    @Test
    public void testClose()
    {
        PersistenceDelegator pd = ((EntityManagerImpl) em).getPersistenceDelegator();
        Assert.assertTrue(pd.isOpen());

        pd.close();
        Assert.assertFalse(pd.isOpen());
        Assert.assertNull(pd.getDelegate());

    }

    @Test
    public void testContains()
    {
        PersonnelDTO dto = new PersonnelDTO();
        dto.setPersonId("123");
        PersistenceDelegator pd = ((EntityManagerImpl) em).getPersistenceDelegator();

        Assert.assertFalse(pd.contains(dto));

        em.persist(dto);
        pd = ((EntityManagerImpl) em).getPersistenceDelegator();
        Assert.assertTrue(pd.contains(dto));

        em.clear();

        pd = ((EntityManagerImpl) em).getPersistenceDelegator();
        Assert.assertFalse(pd.contains(dto));
    }

    @Test
    public void testRefresh()
    {
        PersonnelDTO dto = new PersonnelDTO();
        dto.setPersonId("123");
        PersistenceDelegator pd = ((EntityManagerImpl) em).getPersistenceDelegator();

        try
        {
            pd.refresh(dto);
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertEquals("This is not a valid or managed entity, can't be refreshed", e.getMessage());
        }

        em.persist(dto);
        pd = ((EntityManagerImpl) em).getPersistenceDelegator();

        try
        {
            em.refresh(dto);
        }
        catch (NullPointerException e)
        {
            Assert.assertTrue(true);
        }
        em.clear();

        pd = ((EntityManagerImpl) em).getPersistenceDelegator();
        try
        {
            pd.refresh(dto);
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertEquals("This is not a valid or managed entity, can't be refreshed", e.getMessage());
        }
    }
}
