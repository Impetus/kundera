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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import junit.framework.Assert;

import com.impetus.client.crud.entities.Day;
import com.impetus.client.crud.entities.PersonMongo;
import com.impetus.client.crud.entities.PersonMongo.Month;

/**
 * The Class BaseTest.
 * 
 * @author vivek.mishra
 */
public abstract class BaseTest
{
    /**
     * Prepare data.
     * 
     * @param rowKey
     *            the row key
     * @param age
     *            the age
     * @return the person
     */
    protected PersonMongo prepareMongoInstance(String rowKey, int age)
    {
        PersonMongo o = new PersonMongo();
        o.setPersonId(rowKey);
        o.setPersonName("vivek");
        o.setAge(age);
        o.setDay(Day.FRIDAY);
        o.setMonth(Month.JAN);
        Map<String, Month> map = new HashMap<String, Month>();
        map.put("first month", Month.JAN);
        map.put("second month", Month.FEB);
        o.setMap(map);
        return o;
    }

    /**
     * Find by id.
     * 
     * @param <E>
     *            the element type
     * @param clazz
     *            the clazz
     * @param rowKey
     *            the row key
     * @param em
     *            the em
     * @return the e
     */
    protected <E extends Object> E findById(Class<E> clazz, Object rowKey, EntityManager em)
    {
        return em.find(clazz, rowKey);
    }

    /**
     * Assert find by name.
     * 
     * @param <E>
     *            the element type
     * @param em
     *            the em
     * @param clazz
     *            the clazz
     * @param e
     *            the e
     * @param name
     *            the name
     * @param fieldName
     *            the field name
     */
    protected <E extends Object> void assertFindByName(EntityManager em, String clazz, E e, String name,
            String fieldName)
    {
        String query = "Select p from " + clazz + " p where p." + fieldName + " = " + name;
        // // find by name.
        Query q = em.createQuery(query);
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(3, results.size());
    }

    /**
     * Assert find by name and age.
     * 
     * @param <E>
     *            the element type
     * @param em
     *            the em
     * @param clazz
     *            the clazz
     * @param e
     *            the e
     * @param name
     *            the name
     * @param minVal
     *            the min val
     * @param fieldName
     *            the field name
     */
    protected <E extends Object> void assertFindByNameAndAge(EntityManager em, String clazz, E e, String name,
            String minVal, String fieldName)
    {
        Query q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " = " + name + " and p.age > "
                + minVal);
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(2, results.size());
    }

    /**
     * Assert find by name and age gt and lt.
     * 
     * @param <E>
     *            the element type
     * @param em
     *            the em
     * @param clazz
     *            the clazz
     * @param e
     *            the e
     * @param name
     *            the name
     * @param minVal
     *            the min val
     * @param maxVal
     *            the max val
     * @param fieldName
     *            the field name
     */
    protected <E extends Object> void assertFindByNameAndAgeGTAndLT(EntityManager em, String clazz, E e, String name,
            String minVal, String maxVal, String fieldName)
    {
        // // // find by name, age clause
        Query q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " = " + name + " and p.age > "
                + minVal + " and p.age < " + maxVal);
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(1, results.size());
    }

    /**
     * Assert find by name and age between.
     * 
     * @param <E>
     *            the element type
     * @param em
     *            the em
     * @param clazz
     *            the clazz
     * @param e
     *            the e
     * @param name
     *            the name
     * @param minVal
     *            the min val
     * @param maxVal
     *            the max val
     * @param fieldName
     *            the field name
     */
    protected <E extends Object> void assertFindByNameAndAgeBetween(EntityManager em, String clazz, E e, String name,
            String minVal, String maxVal, String fieldName)
    {
        // // find by between clause
        Query q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " = " + name
                + " and p.age between " + minVal + " and " + maxVal);
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(2, results.size());

    }

    /**
     * Assert find by range.
     * 
     * @param <E>
     *            the element type
     * @param em
     *            the em
     * @param clazz
     *            the clazz
     * @param e
     *            the e
     * @param minVal
     *            the min val
     * @param maxVal
     *            the max val
     * @param fieldName
     *            the field name
     */
    protected <E extends Object> void assertFindByRange(EntityManager em, String clazz, E e, String minVal,
            String maxVal, String fieldName)

    {
        // find by Range.
        Query q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " Between " + minVal + " and "
                + maxVal);
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(2, results.size());
    }

    /**
     * Assert find without where clause.
     * 
     * @param <E>
     *            the element type
     * @param em
     *            the em
     * @param clazz
     *            the clazz
     * @param e
     *            the e
     */
    protected <E extends Object> void assertFindWithoutWhereClause(EntityManager em, String clazz, E e)
    {
        // find by without where clause.
        Query q = em.createQuery("Select p from " + clazz + " p");
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(3, results.size());
    }

    /**
     * Assert on merge.
     * 
     * @param <E>
     *            the element type
     * @param em
     *            the em
     * @param clazz
     *            the clazz
     * @param e
     *            the e
     * @param oldName
     *            the old name
     * @param newName
     *            the new name
     * @param fieldName
     *            the field name
     */
    protected <E extends Object> void assertOnMerge(EntityManager em, String clazz, E e, String oldName,
            String newName, String fieldName)
    {
        Query q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " = '" + oldName+"'");
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " = '" + newName+"'");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertNotSame(oldName, getPersonName(e, results.get(0)));
        Assert.assertEquals(newName, getPersonName(e, results.get(0)));
        
  
        
        q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " like :name");
        q.setParameter("name", "Mc.John Doe");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
     
        q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " like :name");
        q.setParameter("name", "Mc.");
        results = q.getResultList();
        Assert.assertEquals(0, results.size());
        
        q = em.createQuery("Select p from " + clazz + " p where UPPER(p." + fieldName + ") like :name");
        q.setParameter("name", "MC.JOHN DOE");
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        
        q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " like :name");
        q.setParameter("name", "%Doe");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        
        q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " like :name");
        q.setParameter("name", "Mc%");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        
        q = em.createQuery("Select p from " + clazz + " p where (p." + fieldName + " like :name) AND (p.personId like :personId)");
        q.setParameter("name", "Mc%");
        q.setParameter("personId", "1");
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        
        q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " like :name AND p.personId like :personId");
        q.setParameter("name", "Mc%");
        q.setParameter("personId", "2");
        results = q.getResultList();
        Assert.assertEquals(0, results.size());
        
        q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " like :name AND p.personId = :personId");
        q.setParameter("name", "Mc%");
        q.setParameter("personId", "2");
        results = q.getResultList();
        Assert.assertEquals(0, results.size());
        
        q = em.createQuery("Select p from " + clazz + " p where UPPER(p." + fieldName + ") like :name");
        q.setParameter("name", "MC_%");
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        
        q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " like :name");
        q.setParameter("name", "%_Jo%");
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        
        q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " like :name");
        q.setParameter("name", "%_M%");
        results = q.getResultList();
        Assert.assertEquals(0, results.size());
    }

    /**
     * Gets the person name.
     * 
     * @param <E>
     *            the element type
     * @param e
     *            the e
     * @param result
     *            the result
     * @return the person name
     */
    private <E extends Object> String getPersonName(E e, Object result)
    {
        return ((PersonMongo) result).getPersonName();
    }
}
