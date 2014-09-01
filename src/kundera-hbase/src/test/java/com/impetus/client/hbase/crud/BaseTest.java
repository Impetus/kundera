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
package com.impetus.client.hbase.crud;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import junit.framework.Assert;

import com.impetus.client.hbase.crud.PersonHBase.Day;

/**
 * The Class BaseTest.
 * 
 * @author vivek.mishra
 */
public abstract class BaseTest
{
    /**
     * Prepare hbase instance.
     * 
     * @param rowKey
     *            the row key
     * @param age
     *            the age
     * @return the person h base
     */
    protected PersonHBase prepareHbaseInstance(String rowKey, int age)
    {
        PersonHBase o = new PersonHBase();
        o.setPersonId(rowKey);
        o.setPersonName("vivek");
        o.setAge(age);
        o.setDay(Day.MONDAY);
        o.setMonth(Month.MARCH);
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
     * @param em
     * @param clazz
     * @param e
     * @param rowIds
     * @param fieldName
     * @param size
     */
    protected <E extends Object> void assertFindByRowIds(EntityManager em, String clazz, E e, String rowIds, String fieldName, int size)

    {
        // find by Range.
        Query q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " IN ( " + rowIds + " ) ");
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(size, results.size());
    }
    
    /**
     * @param em
     * @param clazz
     * @param e
     * @param fieldValues
     * @param fieldName
     * @param size
     */
    protected <E extends Object> void assertFindByFieldValues(EntityManager em, String clazz, E e, String fieldValues, String fieldName, int size)

    {
        // find by Range.
        Query q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " IN ( " + fieldValues + " ) ");
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(size, results.size());
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
        Query q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " = " + oldName);
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " = " + newName);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertNotSame(oldName, getPersonName(e, results.get(0)));
        Assert.assertEquals(newName, getPersonName(e, results.get(0)));
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
        throw new RuntimeException("Support for " + e + "is not yet supported");
    }

}
