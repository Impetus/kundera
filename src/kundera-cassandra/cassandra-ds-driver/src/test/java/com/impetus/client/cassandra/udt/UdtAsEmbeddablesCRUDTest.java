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
/*
 * @author: karthikp.manchala
 */
package com.impetus.client.cassandra.udt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * The Class UdtAsEmbeddablesCRUDTest.
 */
public class UdtAsEmbeddablesCRUDTest
{

    /** The Constant PU. */
    private static final String PU = "ds_collection_pu";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager entityManager;

    /** The property map. */
    protected static Map propertyMap = null;

    /** The auto manage schema. */
    protected boolean AUTO_MANAGE_SCHEMA = true;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        CassandraCli.cassandraSetUp();
        propertyMap = new HashMap();
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);

        emf = Persistence.createEntityManagerFactory(PU, propertyMap);

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

        entityManager = emf.createEntityManager();

    }

    /**
     * On insert cassandra.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onInsert() throws Exception
    {
        Object p1 = prepareDataLevel1();
        Object p2 = prepareDataLevel2();
        Object p3 = prepareDataLevel3();
        Object p4 = prepareDataLevel4();

        entityManager.persist(p1);
        entityManager.persist(p2);
        entityManager.persist(p3);
        entityManager.persist(p4);
        entityManager.clear();

        PersonUDT f1 = entityManager.find(PersonUDT.class, "1");
        PersonUDT f2 = entityManager.find(PersonUDT.class, "2");
        PersonUDT f3 = entityManager.find(PersonUDT.class, "3");
        PersonUDT f4 = entityManager.find(PersonUDT.class, "4");

        Assert.assertNotNull(f1);
        Assert.assertNotNull(f2);
        Assert.assertNotNull(f3);
        Assert.assertNotNull(f4);

    }

    /**
     * On delete.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onDelete() throws Exception
    {
        Object p1 = prepareDataLevel1();
        Object p2 = prepareDataLevel2();
        Object p3 = prepareDataLevel3();
        Object p4 = prepareDataLevel4();

        entityManager.persist(p1);
        entityManager.persist(p2);
        entityManager.persist(p3);
        entityManager.persist(p4);
        entityManager.clear();

        PersonUDT beforeDelete1 = entityManager.find(PersonUDT.class, "2");
        PersonUDT beforeDelete2 = entityManager.find(PersonUDT.class, "4");
        Assert.assertNotNull(beforeDelete1);
        Assert.assertNotNull(beforeDelete2);

        entityManager.remove(beforeDelete1);
        entityManager.remove(beforeDelete2);
        entityManager.clear();

        PersonUDT after1 = entityManager.find(PersonUDT.class, "2");
        PersonUDT after2 = entityManager.find(PersonUDT.class, "4");

        Assert.assertNull(after1);
        Assert.assertNull(after2);

    }

    /**
     * On merge.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onMerge() throws Exception
    {
        Object p1 = prepareDataLevel1();
        Object p2 = prepareDataLevel2();
        Object p3 = prepareDataLevel3();
        Object p4 = prepareDataLevel4();

        entityManager.persist(p1);
        entityManager.persist(p2);
        entityManager.persist(p3);
        entityManager.persist(p4);
        entityManager.clear();

        PersonUDT beforeMerge = entityManager.find(PersonUDT.class, "3");

        Assert.assertEquals(beforeMerge.getPassword(), ((PersonUDT) p3).getPassword());
        beforeMerge.setPassword("qwerty");
        beforeMerge.getNicknames().add("nick added");
        beforeMerge.setNicknames(beforeMerge.getNicknames());
        entityManager.merge(beforeMerge);
        entityManager.clear();

        PersonUDT afterMerge = entityManager.find(PersonUDT.class, "3");

        Assert.assertNotNull(afterMerge);
        Assert.assertEquals("qwerty", afterMerge.getPassword());
        Assert.assertEquals("nick added", afterMerge.getNicknames().get(1));

    }

    /**
     * On find.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onFind() throws Exception
    {
        Object p1 = prepareDataLevel1();
        Object p2 = prepareDataLevel2();
        Object p3 = prepareDataLevel3();
        Object p4 = prepareDataLevel4();

        entityManager.persist(p1);
        entityManager.persist(p2);
        entityManager.persist(p3);
        entityManager.persist(p4);
        entityManager.clear();

        PersonUDT f1 = entityManager.find(PersonUDT.class, "1");
        PersonUDT f2 = entityManager.find(PersonUDT.class, "2");
        PersonUDT f3 = entityManager.find(PersonUDT.class, "3");
        PersonUDT f4 = entityManager.find(PersonUDT.class, "4");

        Assert.assertNotNull(f1);
        Assert.assertNotNull(f2);
        Assert.assertNotNull(f3);
        Assert.assertNotNull(f4);

        assertEntity((PersonUDT) p1, f1);
        assertEntity((PersonUDT) p2, f2);
        assertEntity((PersonUDT) p3, f3);
        assertEntity((PersonUDT) p4, f4);

    }

    /**
     * On embeddable merge.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onEmbeddableMerge() throws Exception
    {
        Object p1 = prepareDataLevel1();

        entityManager.persist(p1);
        entityManager.clear();

        PersonUDT beforeMerge = entityManager.find(PersonUDT.class, "1");
        entityManager.clear();
        Assert.assertEquals(beforeMerge.getPassword(), ((PersonUDT) p1).getPassword());
        beforeMerge.setPassword("qwerty");
        beforeMerge.getNicknames().add("nick added");
        beforeMerge.setNicknames(beforeMerge.getNicknames());
        PersonalDetailsUDT personalDetails = loadPersonalDetails("devender", "yadav");
        beforeMerge.setPersonalDetails(personalDetails);
        entityManager.merge(beforeMerge);
        entityManager.clear();

        PersonUDT afterMerge = entityManager.find(PersonUDT.class, "1");

        Assert.assertNotNull(afterMerge);
        Assert.assertEquals("qwerty", afterMerge.getPassword());
        Assert.assertEquals("nick added", afterMerge.getNicknames().get(2));
        Assert.assertEquals("devender", afterMerge.getPersonalDetails().getFullname().getFirstName());
        Assert.assertEquals("yadav", afterMerge.getPersonalDetails().getFullname().getLastName());

    }

    /**
     * On select all query.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onSelectAllQuery() throws Exception
    {
        Object p1 = prepareDataLevel1();
        Object p2 = prepareDataLevel2();
        Object p3 = prepareDataLevel3();
        Object p4 = prepareDataLevel4();

        entityManager.persist(p1);
        entityManager.persist(p2);
        entityManager.persist(p3);
        entityManager.persist(p4);
        entityManager.clear();

        PersonUDT f1 = null;
        PersonUDT f2 = null;
        PersonUDT f3 = null;
        PersonUDT f4 = null;

        Query query = entityManager.createQuery("Select t from PersonUDT t");

        List<PersonUDT> results = query.getResultList();
        Iterator<PersonUDT> i = results.iterator();
        while (i.hasNext())
        {
            PersonUDT t = (PersonUDT) i.next();
            if (t.getPersonId().equals("1"))
            {
                f1 = t;
            }
            if (t.getPersonId().equals("2"))
            {
                f2 = t;
            }
            if (t.getPersonId().equals("3"))
            {
                f3 = t;
            }
            if (t.getPersonId().equals("4"))
            {
                f4 = t;
            }
        }

        Assert.assertNotNull(f1);
        Assert.assertNotNull(f2);
        Assert.assertNotNull(f3);
        Assert.assertNotNull(f4);

        assertEntity((PersonUDT) p1, f1);
        assertEntity((PersonUDT) p2, f2);
        assertEntity((PersonUDT) p3, f3);
        assertEntity((PersonUDT) p4, f4);

    }

    /**
     * On select by id query.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onSelectByIdQuery() throws Exception
    {
        Object p3 = prepareDataLevel3();

        entityManager.persist(p3);
        entityManager.clear();

        Query query = entityManager.createQuery("Select t from PersonUDT t where t.personId = 3");

        List<PersonUDT> results = query.getResultList();

        Assert.assertNotNull(results.get(0));

        PersonUDT f3 = results.get(0);

        Assert.assertNotNull(f3);

        assertEntity((PersonUDT) p3, f3);

    }

    /**
     * On collection indexes.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onCollectionIndexes() throws Exception
    {
        Object p3 = prepareDataLevel3();

        entityManager.persist(p3);
        entityManager.clear();

        Query query = entityManager.createQuery("Select t from PersonUDT t where t.nicknames = 'dev'");

        List<PersonUDT> results = query.getResultList();

        Assert.assertNotNull(results.get(0));

        PersonUDT f3 = results.get(0);

        Assert.assertNotNull(f3);

        assertEntity((PersonUDT) p3, f3);

    }
    
    /**
     * Test null embeddable.
     */
    @Test
    public void testNullEmbeddable()
    {
        PersonUDT personUDT = new PersonUDT();
        personUDT.setPersonId("10");
        personUDT.setEmail("user@impetus.com");
        List<String> nicknames = new ArrayList<String>();
        nicknames.add("Charlie");
        nicknames.add("kar");
        personUDT.setNicknames(nicknames);
        personUDT.setPassword("impetus");
        personUDT.setPersonalDetails(null);
        
        entityManager.persist(personUDT);
        entityManager.clear();

        PersonUDT f1 = entityManager.find(PersonUDT.class, "10");
        
        Assert.assertNotNull(f1);
    }

    /**
     * Assert entity.
     * 
     * @param expected
     *            the expected entity
     * @param actual
     *            the actual entity
     */
    private void assertEntity(PersonUDT expected, PersonUDT actual)
    {
        Assert.assertEquals(expected.getEmail(), actual.getEmail());
        Assert.assertEquals(expected.getPassword(), actual.getPassword());
        Assert.assertEquals(expected.getPersonId(), actual.getPersonId());
        Assert.assertEquals(expected.getNicknames(), actual.getNicknames());
        Assert.assertEquals(expected.getPersonalDetails().getFullname().getFirstName(), actual.getPersonalDetails()
                .getFullname().getFirstName());
        Assert.assertEquals(expected.getPersonalDetails().getFullname().getLastName(), actual.getPersonalDetails()
                .getFullname().getLastName());
        assertPersonalDetails(expected.getPersonalDetails(), actual.getPersonalDetails());
        assertProfessionalDetails(expected.getProfessionalDetails(), actual.getProfessionalDetails());
        // assert collection data
        assertProfessionalDetails(expected.getListProfs().get(0), actual.getListProfs().get(0));
        assertProfessionalDetails(expected.getMapProfsKey().get("karthik"), actual.getMapProfsKey().get("karthik"));
        assertProfessionalDetails(expected.getMapProfsKey().get("pragalbh"), actual.getMapProfsKey().get("pragalbh"));
        assertProfessionalDetails(expected.getSetProfs().iterator().next(), actual.getSetProfs().iterator().next());
    }

    /**
     * Assert personal details.
     * 
     * @param expected
     *            the expected
     * @param actual
     *            the actual
     */
    private void assertPersonalDetails(PersonalDetailsUDT expected, PersonalDetailsUDT actual)
    {
        Assert.assertEquals(expected.getAddresses().get("Office").getStreet(), actual.getAddresses().get("Office")
                .getStreet());
        Assert.assertEquals(expected.getAddresses().get("Home").getPin(), actual.getAddresses().get("Home").getPin());
        Assert.assertEquals(expected.getPhones().get(0).getNumber(), actual.getPhones().get(0).getNumber());
        Assert.assertEquals(expected.getPhones().get(0).getTags(), actual.getPhones().get(0).getTags());
        Assert.assertEquals(expected.getSpouses().iterator().next().getMaidenName(), actual.getSpouses().iterator()
                .next().getMaidenName());
    }

    /**
     * Assert professional details.
     * 
     * @param expected
     *            the expected
     * @param actual
     *            the actual
     */
    private void assertProfessionalDetails(ProfessionalDetailsUDT expected, ProfessionalDetailsUDT actual)
    {
        Assert.assertEquals(expected.getCompany(), actual.getCompany());
        Assert.assertEquals(expected.getGrade(), actual.getGrade());
        Assert.assertEquals(expected.getMonthlySalary(), actual.getMonthlySalary());
        // assert collection data
        Assert.assertEquals(expected.getProjects().get(0), actual.getProjects().get(0));
        Assert.assertEquals(expected.getProjects().get(1), actual.getProjects().get(1));
        Assert.assertEquals(true, expected.getColleagues().containsAll(actual.getColleagues()));
        Assert.assertEquals(expected.getExtentions().get(0), actual.getExtentions().get(0));
        Assert.assertEquals(expected.getExtentions().get(1), actual.getExtentions().get(1));

    }

    /**
     * Prepare data level3.
     * 
     * @return the object
     */
    private Object prepareDataLevel3()
    {
        PersonUDT personUDT = new PersonUDT();
        personUDT.setPersonId("3");
        personUDT.setEmail("devender@impetus.com");
        List<String> nicknames = new ArrayList<String>();
        nicknames.add("dev");
        personUDT.setNicknames(nicknames);
        personUDT.setPassword("impetus");
        PersonalDetailsUDT personalDetails = loadPersonalDetails("devender", "yadav");
        personUDT.setPersonalDetails(personalDetails);
        ProfessionalDetailsUDT professionalDetails = loadProfessionalDetails();
        personUDT.setProfessionalDetails(professionalDetails);
        prepareCollectionData(personUDT);

        return personUDT;
    }

    /**
     * Prepare data level4.
     * 
     * @return the object
     */
    private Object prepareDataLevel4()
    {
        PersonUDT personUDT = new PersonUDT();
        personUDT.setPersonId("4");
        personUDT.setEmail("devender@impetus.com");
        List<String> nicknames = new ArrayList<String>();
        nicknames.add("rockstar");
        personUDT.setNicknames(nicknames);
        personUDT.setPassword("impetus");
        PersonalDetailsUDT personalDetails = loadPersonalDetails("amit", "kumar");
        personUDT.setPersonalDetails(personalDetails);
        ProfessionalDetailsUDT professionalDetails = loadProfessionalDetails();
        personUDT.setProfessionalDetails(professionalDetails);
        prepareCollectionData(personUDT);

        return personUDT;
    }

    /**
     * Prepare data level2.
     * 
     * @return the object
     */
    private Object prepareDataLevel2()
    {
        PersonUDT personUDT = new PersonUDT();
        personUDT.setPersonId("2");
        personUDT.setEmail("pragalbh@impetus.com");
        List<String> nicknames = new ArrayList<String>();
        nicknames.add("articuno");
        nicknames.add("PG");
        personUDT.setNicknames(nicknames);
        personUDT.setPassword("impetus");
        PersonalDetailsUDT personalDetails = loadPersonalDetails("pragalbh", "garg");
        personUDT.setPersonalDetails(personalDetails);
        ProfessionalDetailsUDT professionalDetails = loadProfessionalDetails();
        personUDT.setProfessionalDetails(professionalDetails);
        List<ProfessionalDetailsUDT> listProfs = new ArrayList<ProfessionalDetailsUDT>();
        listProfs.add(professionalDetails);
        listProfs.add(professionalDetails);
        personUDT.setListProfs(listProfs);
        prepareCollectionData(personUDT);

        return personUDT;
    }

    /**
     * Prepare data level1.
     * 
     * @return the object
     */
    private Object prepareDataLevel1()
    {
        PersonUDT personUDT = new PersonUDT();
        personUDT.setPersonId("1");
        personUDT.setEmail("karthik@impetus.com");
        List<String> nicknames = new ArrayList<String>();
        nicknames.add("tango");
        nicknames.add("kar");
        personUDT.setNicknames(nicknames);
        personUDT.setPassword("impetus");
        PersonalDetailsUDT personalDetails = loadPersonalDetails("karthik", "manchala");
        personUDT.setPersonalDetails(personalDetails);
        ProfessionalDetailsUDT professionalDetails = loadProfessionalDetails();
        personUDT.setProfessionalDetails(professionalDetails);
        // set collections
        prepareCollectionData(personUDT);

        return personUDT;
    }

    /**
     * Prepare collection data.
     * 
     * @param personUDT
     *            the person udt
     */
    private void prepareCollectionData(PersonUDT personUDT)
    {
        ProfessionalDetailsUDT professionalDetails = loadProfessionalDetails();

        List<ProfessionalDetailsUDT> listProfs = new ArrayList<ProfessionalDetailsUDT>();
        listProfs.add(professionalDetails);
        personUDT.setListProfs(listProfs);

        Map<String, ProfessionalDetailsUDT> mapProfsKey = new HashMap<String, ProfessionalDetailsUDT>();
        mapProfsKey.put("pragalbh", professionalDetails);
        mapProfsKey.put("karthik", professionalDetails);
        personUDT.setMapProfsKey(mapProfsKey);

        Set<ProfessionalDetailsUDT> setProfs = new HashSet<ProfessionalDetailsUDT>();
        setProfs.add(professionalDetails);
        setProfs.add(professionalDetails);
        personUDT.setSetProfs(setProfs);
    }

    /**
     * Load professional details.
     * 
     * @return the professional details udt
     */
    private ProfessionalDetailsUDT loadProfessionalDetails()
    {
        Map<Integer, String> projects = new HashMap<Integer, String>();
        projects.put(1111, "iLabs");
        projects.put(2222, "Kundera");
        List<Integer> extns = new ArrayList<Integer>();
        extns.add(4526);
        extns.add(2810);
        Set<String> colleagues = new HashSet<String>();
        colleagues.add("Pavan");
        colleagues.add("Gautam");
        ProfessionalDetailsUDT profDetails = new ProfessionalDetailsUDT();
        profDetails.setCompany("impetus");
        profDetails.setGrade("g4");
        profDetails.setMonthlySalary((double) 12345);
        profDetails.setProjects(projects);
        profDetails.setColleagues(colleagues);
        profDetails.setExtentions(extns);

        return profDetails;
    }

    /**
     * Load personal details.
     * 
     * @param firstname
     *            the firstname
     * @param lastname
     *            the lastname
     * @return the personal details udt
     */
    private PersonalDetailsUDT loadPersonalDetails(String firstname, String lastname)
    {
        PersonalDetailsUDT personalDetails = new PersonalDetailsUDT();
        Fullname fullname = new Fullname();
        fullname.setFirstName(firstname);
        fullname.setLastName(lastname);
        personalDetails.setFullname(fullname);

        Address address = new Address();
        address.setCity("indore");
        address.setPin("452001");
        address.setStreet("palasia");
        Map<String, Address> addresses = new HashMap<String, Address>();
        addresses.put("Office", address);
        Address address1 = new Address();
        address1.setCity("Delhi");
        address1.setPin("201301");
        address1.setStreet("NSEZ");
        addresses.put("Home", address1);
        personalDetails.setAddresses(addresses);

        Phone phone = new Phone();
        phone.setNumber(9988776655l);
        String elements[] = { "personal", "main", "indore" };
        phone.setTags(new HashSet<String>(Arrays.asList(elements)));
        List<Phone> phoneList = new ArrayList<Phone>();
        phoneList.add(phone);
        personalDetails.setPhones(phoneList);

        Spouse spouse = new Spouse();
        spouse.setAge(20);
        Fullname spouseName = new Fullname();
        spouseName.setFirstName("asdfgh");
        spouse.setFullname(spouseName);
        spouse.setMaidenName("qrqrrte");
        Set<Spouse> spouses = new HashSet<Spouse>();
        spouses.add(spouse);
        personalDetails.setSpouses(spouses);

        return personalDetails;
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
        entityManager.close();
    }

    /**
     * Tear down after class.
     */
    @AfterClass
    public static void tearDownAfterClass()
    {
        CassandraCli.dropKeySpace("KunderaExamples");
        emf.close();
        emf = null;

    }

}
