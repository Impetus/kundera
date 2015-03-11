/**
 * Copyright 2014 Impetus Infotech.
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
package com.impetus.client.hbase.crud.association;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author shaheed.Hussain junit for testing manyToMany in hbase
 * 
 */
public class HbaseManyToManyTest
{

    private EntityManagerFactory emf;

    private EntityManager em;

    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("hbaseTest");
        em = emf.createEntityManager();
    }

    @Test
    public void testManyToMany()
    {
        persistMTM();
        List<HabitatMToM> habitatMToM = em.createQuery("Select h from HabitatMToM h").getResultList();
        assertNotNull(habitatMToM);
        assertEquals(3, habitatMToM.size());

        List<PersonnelMToM> personnelMToM = em.createQuery("Select p from PersonnelMToM p").getResultList();
        assertNotNull(personnelMToM);
        assertEquals(2, personnelMToM.size());

        personnelMToM = em.createQuery("Select p from PersonnelMToM p where p.personName=John").getResultList();
        assertEquals(personnelMToM.get(0).getPersonId(), "125");
        assertNotNull(personnelMToM.get(0).getHabitats().iterator().next());

        HabitatMToM habitatMToM1 = em.find(HabitatMToM.class, "7");
        HabitatMToM habitatMToM2 = em.find(HabitatMToM.class, "8");
        HabitatMToM habitatMToM3 = em.find(HabitatMToM.class, "9");

        PersonnelMToM personnelMToM1 = em.find(PersonnelMToM.class, "125");
        PersonnelMToM personnelMToM2 = em.find(PersonnelMToM.class, "502");

        assertEquals(habitatMToM1.getStreet(), "downing street");
        assertEquals(habitatMToM2.getStreet(), "fleet street");
        assertEquals(habitatMToM3.getStreet(), "greet street");

        assertEquals(personnelMToM1.getPersonName(), "John");
        assertEquals(personnelMToM2.getPersonName(), "Bully");

        assertEquals(2, personnelMToM1.getHabitats().size());
        for (HabitatMToM habitat : personnelMToM1.getHabitats())
        {
            if (habitat.getAddressId().equals("7"))
            {
                assertEquals(habitat.getStreet(), "downing street");
            }
            else
            {
                assertEquals(habitat.getStreet(), "fleet street");
            }
        }

    }

    private void persistMTM()
    {
        Set<HabitatMToM> habitats1 = new HashSet<HabitatMToM>();
        Set<HabitatMToM> habitats2 = new HashSet<HabitatMToM>();

        HabitatMToM habitat1 = new HabitatMToM();
        habitat1.setAddressId("7");
        habitat1.setStreet("downing street");

        HabitatMToM habitat2 = new HabitatMToM();
        habitat2.setAddressId("8");
        habitat2.setStreet("fleet street");

        HabitatMToM habitat3 = new HabitatMToM();
        habitat3.setAddressId("9");
        habitat3.setStreet("greet street");

        habitats1.add(habitat1);
        habitats1.add(habitat2);

        habitats2.add(habitat2);
        habitats2.add(habitat3);

        PersonnelMToM person1 = new PersonnelMToM();
        person1.setPersonId("125");
        person1.setPersonName("John");

        PersonnelMToM person2 = new PersonnelMToM();
        person2.setPersonId("502");
        person2.setPersonName("Bully");

        person1.setHabitats(habitats1);
        person2.setHabitats(habitats2);

        em.persist(person1);
        em.persist(person2);
    }

    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
    }

}
