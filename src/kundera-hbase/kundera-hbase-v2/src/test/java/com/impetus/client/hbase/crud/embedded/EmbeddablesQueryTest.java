/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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

package com.impetus.client.hbase.crud.embedded;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;

/**
 * The Class EmbeddablesQueryTest.
 * 
 * @author Pragalbh Garg
 */
public class EmbeddablesQueryTest extends EmbeddablesBase
{

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void SetUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(HBASE_PU);
    }

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
        init();

    }

    /**
     * Test select queries.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void testSelectQueries() throws Exception
    {
        persistData();
        testSelectSimple();
        testSelectWithWhere();
        testSelectWithWhereIn();

    }

    /**
     * Test delete queries.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void testDeleteQueries() throws Exception
    {
        testDeleteSimple();
        testDeleteWithWhere();
        testDeleteWithWhereIn();

    }

    // update is not working on embeddables. issue in core.
    // @Test
    /**
     * Test update queries.
     * 
     * @throws Exception
     *             the exception
     */
    public void testUpdateQueries() throws Exception
    {
        testUpdateQuery();

    }

    /**
     * Test select simple.
     */
    private void testSelectSimple()
    {
        List<PersonEmbed> results = em.createQuery("select p from PersonEmbed p").getResultList();
        assertResults(results, T, T, T, T);
    }

    /**
     * Test select with where.
     */
    private void testSelectWithWhere()
    {
        List<PersonEmbed> results = em.createQuery("select p from PersonEmbed p where p.email = 'pg@gmail.com'")
                .getResultList();
        assertResults(results, T, F, F, F);

        results = em.createQuery("select p from PersonEmbed p where p.personal.fullname.firstName = 'dev'")
                .getResultList();
        assertResults(results, F, T, F, F);

        results = em.createQuery("select p from PersonEmbed p where p.personal.fullname.firstName <> 'pragalbh'")
                .getResultList();
        assertResults(results, F, T, T, T);

        results = em.createQuery("select p from PersonEmbed p where p.professional.company='impetus'").getResultList();
        assertResults(results, T, T, T, T);

        results = em.createQuery("select p from PersonEmbed p where p.professional.salary = 50000").getResultList();
        assertResults(results, F, F, T, T);

        results = em.createQuery("select p from PersonEmbed p where p.professional.salary < 50000").getResultList();
        assertResults(results, T, T, F, F);

        results = em
                .createQuery(
                        "select p from PersonEmbed p where p.personal.fullname.firstName = 'dev' and p.professional.salary = 50000")
                .getResultList();
        assertResults(results, F, F, F, F);

        results = em
                .createQuery(
                        "select p from PersonEmbed p where p.professional.company='impetus' and p.personal.fullname.middleName = 'star'")
                .getResultList();
        assertResults(results, F, F, F, T);

        results = em
                .createQuery(
                        "select p from PersonEmbed p where p.professional.salary = 50000 or p.personal.fullname.lastName = 'garg'")
                .getResultList();
        assertResults(results, T, F, T, T);

        ProfessionalDetails pro = new ProfessionalDetails();
        pro.setCompany("impetus");
        pro.setMonthlySalary((double) 50000);
        pro.setProject("kundera");
        Query qry = em.createQuery("select p from PersonEmbed p where p.professional = :pro");
        qry.setParameter("pro", pro);
        results = qry.getResultList();
        assertResults(results, F, F, T, T);
    }

    /**
     * Test select with where in.
     */
    private void testSelectWithWhereIn()
    {
        List<PersonEmbed> results = em.createQuery(
                "select p from PersonEmbed p where p.personal.fullname.firstName in ('pragalbh','amit')")
                .getResultList();
        assertResults(results, T, F, F, T);

        results = em.createQuery("select p from PersonEmbed p where p.professional.salary in (40000,50000)")
                .getResultList();
        assertResults(results, T, T, T, T);

        List<String> names = new ArrayList<String>();
        names.add("amit");
        names.add("cherry");
        names.add("dev");

        Query query = em.createQuery("select p from PersonEmbed p where p.personal.fullname.firstName in :names");
        query.setParameter("names", names);
        results = query.getResultList();
        assertResults(results, F, T, F, T);

    }

    /**
     * Test delete simple.
     */
    private void testDeleteSimple()
    {
        persistData();
        int result = em.createQuery("delete from PersonEmbed p").executeUpdate();
        Assert.assertEquals(4, result);
        assertDeleted(T, T, T, T);

    }

    /**
     * Test delete with where.
     */
    private void testDeleteWithWhere()
    {
        persistData();
        int result = em.createQuery("delete from PersonEmbed p where p.email = 'pg@gmail.com'").executeUpdate();
        Assert.assertEquals(1, result);
        assertDeleted(T, F, F, F);

        persistData();
        result = em.createQuery("delete from PersonEmbed p where p.personal.fullname.firstName <> 'pragalbh'")
                .executeUpdate();
        Assert.assertEquals(3, result);
        assertDeleted(F, T, T, T);

        persistData();
        result = em.createQuery("delete from PersonEmbed p where p.professional.salary < 50000").executeUpdate();
        Assert.assertEquals(2, result);
        assertDeleted(T, T, F, F);

        persistData();
        result = em
                .createQuery(
                        "delete from PersonEmbed p where p.personal.fullname.firstName = 'dev' and p.professional.salary = 50000")
                .executeUpdate();
        Assert.assertEquals(0, result);
        assertDeleted(F, F, F, F);

        persistData();
        result = em
                .createQuery(
                        "delete from PersonEmbed p where p.professional.company='impetus' and p.personal.fullname.middleName = 'star'")
                .executeUpdate();
        Assert.assertEquals(1, result);
        assertDeleted(F, F, F, T);

    }

    /**
     * Test delete with where in.
     */
    private void testDeleteWithWhereIn()
    {
        persistData();
        int result = em.createQuery(
                "delete from PersonEmbed p where p.personal.fullname.firstName in ('pragalbh','amit')").executeUpdate();
        assertDeleted(T, F, F, T);

        persistData();
        result = em.createQuery("delete from PersonEmbed p where p.professional.salary in (40000,50000)")
                .executeUpdate();
        assertDeleted(T, T, T, T);

        List<String> names = new ArrayList<String>();
        names.add("amit");
        names.add("cherry");
        names.add("dev");

        persistData();
        Query query = em.createQuery("delete from PersonEmbed p where p.personal.fullname.firstName in :names");
        query.setParameter("names", names);
        result = query.executeUpdate();
        assertDeleted(F, T, F, T);

    }

    /**
     * Test update query.
     */
    private void testUpdateQuery()
    {
        persistData();
        int result = em.createQuery("update PersonEmbed p set p.personal.fullname.firstName = 'newname'")
                .executeUpdate();
        Assert.assertEquals(4, result);
        em.clear();
        PersonEmbed person1 = em.find(PersonEmbed.class, 1);
        PersonEmbed person2 = em.find(PersonEmbed.class, 2);
        PersonEmbed person3 = em.find(PersonEmbed.class, 3);
        PersonEmbed person4 = em.find(PersonEmbed.class, 4);
        Assert.assertEquals("newname", person1.getPersonalDetails().getFullname().getFirstName());
        Assert.assertEquals("newname", person2.getPersonalDetails().getFullname().getFirstName());
        Assert.assertEquals("newname", person3.getPersonalDetails().getFullname().getFirstName());
        Assert.assertEquals("newname", person4.getPersonalDetails().getFullname().getFirstName());
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
    }

    /**
     * Tear down after class.
     */
    @AfterClass
    public static void tearDownAfterClass()
    {
        HBaseTestingUtils.dropSchema(SCHEMA);
        emf.close();
        emf = null;
    }

}