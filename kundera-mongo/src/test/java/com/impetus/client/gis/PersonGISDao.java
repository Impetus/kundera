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

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import com.impetus.client.utils.MongoUtils;
import com.impetus.kundera.gis.geometry.Circle;
import com.impetus.kundera.gis.geometry.Coordinate;
import com.impetus.kundera.gis.geometry.Envelope;
import com.impetus.kundera.gis.geometry.Polygon;
import com.impetus.kundera.gis.geometry.Triangle;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;

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
        return em.find(Person.class, personId);
    }

    public List<Person> findWithinCircle(double x, double y, double r)
    {
        Circle circle = new Circle(x, y, r);

        Query q = em.createQuery("Select p from Person p where p.currentLocation IN :circle");
        q.setParameter("circle", circle);
        List<Person> persons = q.getResultList();
        return persons;
    }

    public void mergePerson(Person person)
    {
        em.merge(person);
    }

    public void removePerson(Person person)
    {
        em.remove(person);
    }

    /**
     * @param d
     * @param e
     * @param f
     * @param i
     * @param h
     * @param g
     * @return
     */
    public List<Person> findWithinTriangle(double x1, double y1, double x2, double y2, double x3, double y3)
    {

        Triangle triangle = new Triangle(x1, y1, x2, y2, x3, y3);

        Query q = em.createQuery("Select p from Person p where p.currentLocation IN :triangle");
        q.setParameter("triangle", triangle);
        List<Person> persons = q.getResultList();
        return persons;
    }

    /**
     * 
     * @param shell
     * @param holes
     * @param factory
     * @return
     */
    public List<Person> findWithinPolygon(Polygon polygon)
    {
        Query q = em.createQuery("Select p from Person p where p.currentLocation IN :polygon");
        q.setParameter("polygon", polygon);
        List<Person> persons = q.getResultList();
        return persons;
    }

    /**
     * @param d
     * @param e
     * @param f
     * @param g
     * @param h
     * @param i
     * @param j
     * @param k
     * @return
     */
    public List<Person> findWithinRectangle(double x1, double y1, double x2, double y2)
    {
        Envelope envelope = new Envelope(x1, x2, y1, y2);
        Query q = em.createQuery("Select p from Person p where p.currentLocation IN :envelope");
        q.setParameter("envelope", envelope);
        List<Person> persons = q.getResultList();
        return persons;
    }

}
