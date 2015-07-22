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

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Root;

import junit.framework.Assert;

import com.impetus.client.crud.PersonCassandra.Day;

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
    protected PersonCassandra prepareData(String rowKey, int age)
    {
        PersonCassandra o = new PersonCassandra();
        o.setPersonId(rowKey);
        o.setPersonName("vivek");
        o.setAge(age);
        o.setDay(Day.thursday);
        o.setMonth(Month.APRIL);
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
    protected <E extends Object> void assertFindByName(EntityManager em, Class clazz, E e, String name, String fieldName)
    {

        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<E> query = criteriaBuilder.createQuery(clazz);
        Root<E> from = query.from(clazz);
        query.select(from.alias("p"));
        query.where(criteriaBuilder.equal(from.get(fieldName), name));
        // // find by name.
        TypedQuery<E> q = em.createQuery(query);
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(3, results.size());
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
    protected <E extends Object> void assertFindByNameWithReservedKeywords(EntityManager em, Class clazz, E e, String name, String fieldName)
    {

        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<E> query = criteriaBuilder.createQuery(clazz);
        Root<E> from = query.from(clazz);
        query.select(from.alias("p"));
        query.where(criteriaBuilder.equal(from.get(fieldName), name));
        // // find by name.
        TypedQuery<E> q = em.createQuery(query);
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        Assert.assertEquals(1, results.size());
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

    protected <E extends Object> void assertFindByNameAndAge(EntityManager em, Class clazz, E e, String name,
            String minVal, String fieldName)
    {

        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<E> query = criteriaBuilder.createQuery(clazz);
        Root<E> from = query.from(clazz);
        query.select(from.alias("p"));
        query.where(criteriaBuilder.and(criteriaBuilder.equal(from.get(fieldName), name),
                criteriaBuilder.gt((Expression) from.get("age"), Integer.parseInt(minVal))));

        TypedQuery<E> q = em.createQuery(query);
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

    protected <E extends Object> void assertFindByNameAndAgeGTAndLT(EntityManager em, Class clazz, E e, String name,
            String minVal, String maxVal, String fieldName)
    {
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<E> query = criteriaBuilder.createQuery(clazz);
        Root<E> from = query.from(clazz);
        query.select(from.alias("p"));
        query.where(criteriaBuilder.and(criteriaBuilder.equal(from.get(fieldName), name),
                criteriaBuilder.gt((Expression) from.get("age"), Integer.parseInt(minVal)),
                criteriaBuilder.lt((Expression) from.get("age"), Integer.parseInt(maxVal))));

        // // // find by name, age clause
        TypedQuery<E> q = em.createQuery(query);
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

    protected <E extends Object> void assertFindByNameAndAgeBetween(EntityManager em, Class clazz, E e, String name,
            String minVal, String maxVal, String fieldName)
    {
        // // find by between clause
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<E> query = criteriaBuilder.createQuery(clazz);
        Root<E> from = query.from(clazz);
        query.select(from.alias("p"));
        query.where(criteriaBuilder.and(
                criteriaBuilder.equal(from.get(fieldName), name),
                criteriaBuilder.between((Expression) from.get("age"), Integer.parseInt(minVal),
                        Integer.parseInt(maxVal))));

        TypedQuery<E> q = em.createQuery(query);
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
            String maxVal, String fieldName, boolean useCql)

    {
        // find by Range.
        Query q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " Between " + minVal + " and "
                + maxVal);
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        if (useCql)
        {
            Assert.assertEquals(3, results.size());
        }
        else
        {
            Assert.assertEquals(2, results.size());
        }
    }

    protected <E extends Object> void assertFindByRange(EntityManager em, Class clazz, E e, String minVal,
            String maxVal, String fieldName, boolean useCql)

    {
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<E> query = criteriaBuilder.createQuery(clazz);
        Root<E> from = query.from(clazz);
        query.select(from.alias("p"));
        query.where(criteriaBuilder.between((Expression) from.get(fieldName), Integer.parseInt(minVal),
                Integer.parseInt(maxVal)));

        TypedQuery<E> q = em.createQuery(query);
        List<E> results = q.getResultList();

        // find by Range.
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());

        if (useCql)
        {
            Assert.assertEquals(3, results.size());
        }
        else
        {
            Assert.assertEquals(2, results.size());
        }
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
    protected <E extends Object> void assertFindWithoutWhereClause(EntityManager em, String clazz, E e, boolean useCql)
    {
        // find by without where clause.
        Query q = em.createQuery("Select p from " + clazz + " p");
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        if (useCql)
        {
            Assert.assertEquals(4, results.size());
        }
        else
        {
            Assert.assertEquals(3, results.size());
        }
    }

    protected <E extends Object> void assertFindWithoutWhereClause(EntityManager em, Class clazz, E e, boolean useCql)
    {
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<E> query = criteriaBuilder.createQuery(clazz);
        Root<E> from = query.from(clazz);
        query.select(from.alias("p"));

        // find by without where clause.
        TypedQuery<E> q = em.createQuery(query);
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertFalse(results.isEmpty());
        if (useCql)
        {
            Assert.assertEquals(4, results.size());
        }
        else
        {
            Assert.assertEquals(3, results.size());
        }
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

    protected <E extends Object> void assertOnMerge(EntityManager em, Class clazz, E e, String oldName, String newName,
            String fieldName)
    {
        CriteriaBuilder criteriaBuilder = em.getCriteriaBuilder();
        CriteriaQuery<E> query = criteriaBuilder.createQuery(clazz);
        Root<E> from = query.from(clazz);
        query.select(from.alias("p"));
        query.where(criteriaBuilder.equal(from.get(fieldName), oldName));
        // // find by name.
        TypedQuery<E> q = em.createQuery(query);
        List<E> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());

        criteriaBuilder = em.getCriteriaBuilder();
        query = criteriaBuilder.createQuery(clazz);
        from = query.from(clazz);
        query.select(from.alias("p"));
        query.where(criteriaBuilder.equal(from.get(fieldName), newName));
        // // find by name.
        q = em.createQuery(query);
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

        return ((PersonCassandra) result).getPersonName();
    }

}
