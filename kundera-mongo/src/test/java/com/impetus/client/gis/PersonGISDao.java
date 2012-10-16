/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.gis;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import com.impetus.client.utils.MongoUtils;

/**
 * DAO for Geolocation processing for {@link Person} entity
 * 
 * @author amresh.singh
 */
public class PersonGISDao
{

    private EntityManager em;

    private EntityManagerFactory emf;

    private String pu;

    public PersonGISDao(String persistenceUnitName)
    {
        this.pu = persistenceUnitName;
        if (emf == null)
        {
            try
            {
                emf = Persistence.createEntityManagerFactory(persistenceUnitName);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

    }

    public void createEntityManager()
    {
        if (em == null)
        {
            em = emf.createEntityManager();
        }
    }

    public void closeEntityManager()
    {
        if (em != null)
        {
            em.close();
            em = null;
        }
    }

    public void close()
    {
        if (emf != null)
        {
            MongoUtils.dropDatabase(emf, pu);
            emf.close();
        }
    }

    public void addPerson(Person person)
    {
        em.persist(person);
    }

    public Person findPerson(int personId)
    {
        return em.find(Person.class, 1);
    }

    public void removePerson(Person person)
    {
        em.remove(person);
    }

    public void mergePerson(Person person)
    {
        em.merge(person);
    }

}
