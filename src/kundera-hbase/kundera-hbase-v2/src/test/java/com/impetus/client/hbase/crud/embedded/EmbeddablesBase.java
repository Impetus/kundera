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

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import junit.framework.Assert;

/**
 * The Class EmbeddablesBase.
 *
 * @author Pragalbh Garg
 */
public class EmbeddablesBase
{
    /** The Constant SCHEMA. */
    protected static final String SCHEMA = "HBaseNew";

    /** The Constant HBASE_PU. */
    protected static final String HBASE_PU = "embeddablesTest";

    /** The emf. */
    protected static EntityManagerFactory emf;

    /** The em. */
    protected static EntityManager em;

    /** The Constant T. */
    protected static final boolean T = true;

    /** The Constant F. */
    protected static final boolean F = false;

    /** The p4. */
    protected PersonEmbed p1, p2, p3, p4;

    /**
     * Inits the.
     */
    protected void init()
    {
        this.p1 = getPerson_1();
        this.p2 = getPerson_2();
        this.p3 = getPerson_3();
        this.p4 = getPerson_4();
    }

    /**
     * Prepare person.
     *
     * @param id the id
     * @param email the email
     * @param details1 the details1
     * @param details2 the details2
     * @return the person embed
     */
    protected PersonEmbed preparePerson(int id, String email, PersonalDetails details1, ProfessionalDetails details2)
    {
        PersonEmbed p = new PersonEmbed();
        p.setPersonId(id);
        p.setEmail(email);
        p.setPersonalDetails(details1);
        p.setProfessionalDetails(details2);
        return p;
    }

    /**
     * Prepare personal details.
     *
     * @param fname the fname
     * @param mname the mname
     * @param lname the lname
     * @param address the address
     * @return the personal details
     */
    protected PersonalDetails preparePersonalDetails(String fname, String mname, String lname, List address)
    {
        PersonalDetails p = new PersonalDetails();
        Fullname f = new Fullname();
        f.setFirstName(fname);
        f.setMiddleName(mname);
        f.setLastName(lname);
        p.setFullname(f);
        p.setAddresses(address);
        return p;
    }

    /**
     * Prepare pro details.
     *
     * @param project the project
     * @param comp the comp
     * @param salary the salary
     * @return the professional details
     */
    protected ProfessionalDetails prepareProDetails(String project, String comp, Double salary)
    {
        ProfessionalDetails p = new ProfessionalDetails();
        p.setCompany(comp);
        p.setMonthlySalary(salary);
        p.setProject(project);
        return p;
    }

    /**
     * Gets the person_1.
     *
     * @return the person_1
     */
    protected PersonEmbed getPerson_1()
    {
        ProfessionalDetails pro = prepareProDetails("kundera", "impetus", (double) 40000);
        Address add1 = new Address();
        add1.setCity("indore");
        add1.setPin("452001");
        add1.setStreet("palasia");

        Address add2 = new Address();
        add2.setCity("gwalior");
        add2.setPin("474011");
        add2.setStreet("thatipur");

        List<Address> address = new ArrayList<Address>();
        address.add(add1);
        address.add(add2);
        PersonalDetails personal = preparePersonalDetails("pragalbh", "rocking", "garg", address);

        return preparePerson(1, "pg@gmail.com", personal, pro);
    }

    /**
     * Gets the person_2.
     *
     * @return the person_2
     */
    protected PersonEmbed getPerson_2()
    {
        ProfessionalDetails pro = prepareProDetails("kundera", "impetus", (double) 40000);
        Address add1 = new Address();
        add1.setCity("noida");
        add1.setPin("100100");
        add1.setStreet("k block");

        Address add2 = new Address();
        add2.setCity("delhi");
        add2.setPin("100200");
        add2.setStreet("cp");

        List<Address> address = new ArrayList<Address>();
        address.add(add1);
        address.add(add2);
        PersonalDetails personal = preparePersonalDetails("dev", "cool", "yadav", address);

        return preparePerson(2, "dev@gmail.com", personal, pro);
    }

    /**
     * Gets the person_3.
     *
     * @return the person_3
     */
    protected PersonEmbed getPerson_3()
    {
        ProfessionalDetails pro = prepareProDetails("kundera", "impetus", (double) 50000);
        Address add1 = new Address();
        add1.setCity("indore");
        add1.setPin("452001");
        add1.setStreet("bengali");

        Address add2 = new Address();
        add2.setCity("hyderabad");
        add2.setPin("200020");
        add2.setStreet("some street");

        List<Address> address = new ArrayList<Address>();
        address.add(add1);
        address.add(add2);
        PersonalDetails personal = preparePersonalDetails("karthik", "cherry", "manchala", address);

        return preparePerson(3, "karthik@gmail.com", personal, pro);
    }

    /**
     * Gets the person_4.
     *
     * @return the person_4
     */
    protected PersonEmbed getPerson_4()
    {
        ProfessionalDetails pro = prepareProDetails("kundera", "impetus", (double) 50000);
        Address add1 = new Address();
        add1.setCity("noida");
        add1.setPin("100100");
        add1.setStreet("k block");

        Address add2 = new Address();
        add2.setCity("delhi");
        add2.setPin("100300");
        add2.setStreet("karol bag");

        List<Address> address = new ArrayList<Address>();
        address.add(add1);
        address.add(add2);
        PersonalDetails personal = preparePersonalDetails("amit", "star", "kumar", address);

        return preparePerson(4, "amit@gmail.com", personal, pro);
    }

    /**
     * Assert person.
     *
     * @param expected the expected
     * @param actual the actual
     */
    protected void assertPerson(PersonEmbed expected, PersonEmbed actual)
    {
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.getPersonId(), actual.getPersonId());
        Assert.assertEquals(expected.getEmail(), actual.getEmail());
        Assert.assertNotNull(actual.getPersonalDetails());
        Assert.assertEquals(expected.getProfessionalDetails().getCompany(), actual.getProfessionalDetails()
                .getCompany());
        Assert.assertEquals(expected.getProfessionalDetails().getProject(), actual.getProfessionalDetails()
                .getProject());
        Assert.assertEquals(expected.getProfessionalDetails().getMonthlySalary(), actual.getProfessionalDetails()
                .getMonthlySalary());
        Fullname exp = expected.getPersonalDetails().getFullname();
        Fullname act = actual.getPersonalDetails().getFullname();
        Assert.assertNotNull(act);
        Assert.assertEquals(exp.getFirstName(), act.getFirstName());
        Assert.assertEquals(exp.getMiddleName(), act.getMiddleName());
        Assert.assertEquals(exp.getLastName(), act.getLastName());
        List<Address> expAdd = expected.getPersonalDetails().getAddresses();
        List<Address> actAdd = actual.getPersonalDetails().getAddresses();
        Assert.assertNotNull(actAdd);
        Assert.assertEquals(expAdd.size(), actAdd.size());

    }

    /**
     * Persist data.
     */
    protected void persistData()
    {
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        em.persist(p4);
        em.clear();
    }

    /**
     * Assert results.
     *
     * @param results the results
     * @param b1 the b1
     * @param b2 the b2
     * @param b3 the b3
     * @param b4 the b4
     */
    protected void assertResults(List results, Boolean b1, Boolean b2, Boolean b3, Boolean b4)
    {
        Assert.assertNotNull(results);
        for (PersonEmbed p : (List<PersonEmbed>) results)
        {
            switch (p.getPersonId())
            {
            case 1:
                if (b1)
                    assertPerson(p1, p);
                else
                    Assert.assertTrue(false);
                break;
            case 2:
                if (b2)
                    assertPerson(p2, p);
                else
                    Assert.assertTrue(false);
                break;
            case 3:
                if (b3)
                    assertPerson(p3, p);
                else
                    Assert.assertTrue(false);
                break;
            case 4:
                if (b4)
                    assertPerson(p4, p);
                else
                    Assert.assertTrue(false);
                break;
            }

        }
    }

    /**
     * Assert deleted.
     *
     * @param b1 the b1
     * @param b2 the b2
     * @param b3 the b3
     * @param b4 the b4
     */
    protected void assertDeleted(Boolean b1, Boolean b2, Boolean b3, Boolean b4)
    {
        em.clear();
        PersonEmbed person1 = em.find(PersonEmbed.class, 1);
        PersonEmbed person2 = em.find(PersonEmbed.class, 2);
        PersonEmbed person3 = em.find(PersonEmbed.class, 3);
        PersonEmbed person4 = em.find(PersonEmbed.class, 4);
        if (b1)
        {
            Assert.assertNull(person1);
        }
        else
        {
            Assert.assertNotNull(person1);
        }
        if (b2)
        {
            Assert.assertNull(person2);
        }
        else
        {
            Assert.assertNotNull(person2);
        }
        if (b3)
        {
            Assert.assertNull(person3);
        }
        else
        {
            Assert.assertNotNull(person3);
        }
        if (b4)
        {
            Assert.assertNull(person4);
        }
        else
        {
            Assert.assertNotNull(person4);
        }
    }

}
