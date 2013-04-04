/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.oraclenosql;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.examples.crud.PersonKVStore;

/**
 * <Prove description of functionality provided by this Type> 
 * @author amresh.singh
 */

public class EntityPersistenceKVStoreTest extends BaseKVStoreTest {

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    @Test
    public void dummyTest()
    {
        
    }
    
    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    //@Before
    public void setUp() throws Exception {
        super.setup();
        if (store == null) {
            System.err.println("Skippping integration test");
        }
        assumeNotNull(store);
        emf = Persistence.createEntityManagerFactory("twikvstore");
        // "twissandra,twikvstore,twingo,twibase");
        em = emf.createEntityManager();

    }

    protected PersonKVStore prepareKVstoreData(String rowKey, int age, String name) {
        PersonKVStore o = new PersonKVStore();
        o.setPersonId(rowKey);
        o.setPersonName(name);
        o.setAge(age);
        return o;
    }

    protected <E extends Object> E findById(Class<E> clazz, Object rowKey, EntityManager em) {
        return em.find(clazz, rowKey);
    }

    /**
     * On insert cassandra.
     */
    //@Test
    public void onInsertOraKVStore() {
        assumeNotNull(store);
        Object p1 = prepareKVstoreData("1", 10, "person1");
        Object p2 = prepareKVstoreData("2", 20, "person2");
        Object p3 = prepareKVstoreData("3", 15, "person3");
        Object p4 = prepareKVstoreData("4", 15, "person1");
        em.persist(p1);
        em.persist(p2);
        em.persist(p3);
        em.persist(p4);
        // col.put("1", p1);
        // col.put("2", p2);
        // col.put("3", p3);

        PersonKVStore p = findById(PersonKVStore.class, "1", em);
        assertNotNull(p);
        assertEquals("person1", p.getPersonName());
        assertEquals(10, p.getAge());
        p = findById(PersonKVStore.class, "2", em);
        assertNotNull(p);
        assertEquals("person2", p.getPersonName());
        assertEquals(20, p.getAge());

        assertFindByName(em, "PersonKVStore", PersonKVStore.class, "person1", "PERSON_NAME");
        assertFindByNameAndAge(em, "PersonKVStore", PersonKVStore.class, "person1", "10", "PERSON_NAME");
//         assertFindByNameAndAgeGTAndLT(em, "PersonKVStore",
//                 PersonKVStore.class, "person1", "5", "25", "PERSON_NAME");
        // assertFindByNameAndAgeBetween(em, "PersonCassandra",
        // PersonCassandra.class, "vivek", "10", "15", "PERSON_NAME");
        // assertFindByRange(em, "PersonCassandra", PersonCassandra.class, "10",
        // "20", "PERSON_ID");
        // assertFindWithoutWhereClause(em, "PersonCassandra",
        // PersonCassandra.class);
    }

    protected <E extends Object> void assertFindByNameAndAge(EntityManager em, String clazz, E e, String name, String minVal, String fieldName) {
        Query q = em.createQuery("Select p from " + clazz + " p where p." + fieldName + " = " + name + " and p.AGE = " + minVal);
        List<E> results = q.getResultList();
        assertNotNull(results);
        assertTrue(!results.isEmpty());
        assertEquals(1, results.size());
    }

    protected <E extends Object> void assertFindByName(EntityManager em, String clazz, E e, String name, String fieldName) {

        String query = "Select p from " + clazz + " p where p." + fieldName + " = " + name;
        // // find by name.
        Query q = em.createQuery(query);
        List<E> results = q.getResultList();
        assertNotNull(results);
        assertTrue(!results.isEmpty());
        assertEquals(2, results.size());

    }
    protected <E extends Object> void assertFindByNameAndAgeGTAndLT(EntityManager em, String clazz, E e, String name, String minVal, String maxVal, String fieldName)
    {
        // // // find by name, age clause
        Query q = em.createQuery("Select p from " + clazz
                + " p where p."+fieldName+" = " + name + " and p.AGE > "+ minVal+ " and p.AGE < " +maxVal);
        List<E> results = q.getResultList();
        assertNotNull(results);
//        assertTrue(!results.isEmpty());
        assertEquals(2, results.size());
    }
}

