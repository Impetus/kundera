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
package com.impetus.client.crud;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * Many to many association test.
 * 
 * @author vivek.mishra
 */
public class ManyToManyAssociationTest
{

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    private static final String _PU = "ds_pu";

    @Before
    public void setup() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.dropKeySpace("KunderaExamples");
        CassandraCli.createKeySpace("KunderaExamples");
        Map propertyMap = new HashMap();
//        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "");

        emf = Persistence.createEntityManagerFactory(_PU, propertyMap);
        em = emf.createEntityManager();
//        createSchema();
    }

    @Test
    public void testManyToMany()
    {
        StudentMTM student1 = new StudentMTM(100l);
        student1.setFirstName("vivek");
        student1.setLastName("mishra");

        StudentMTM student2 = new StudentMTM(101l);
        student2.setFirstName("kuldeep");
        student2.setLastName("mishra");

        StudentMTM student3 = new StudentMTM(102l);
        student3.setFirstName("chhavi");
        student3.setLastName("gangwal");

        StudentMTM student4 = new StudentMTM(103l);
        student4.setFirstName("shaheed");
        student4.setLastName("hussain");

        ClassMTM mathClass = new ClassMTM(200l);
        mathClass.setTopic("maths");

        ClassMTM scienceClass = new ClassMTM(201l);
        scienceClass.setTopic("science");

        ClassMTM electricalClass = new ClassMTM(202l);
        electricalClass.setTopic("electrical");

        student1.assignClass(mathClass);
        // mathClass.assignStudent(student1);

        student2.assignClass(mathClass);
        student2.assignClass(scienceClass);
        // mathClass.assignStudent(student2);
        // scienceClass.assignStudent(student2);

        student3.assignClass(mathClass);
        student3.assignClass(scienceClass);
        // mathClass.assignStudent(student3);
        // scienceClass.assignStudent(student3);

        student4.assignClass(mathClass);
        student4.assignClass(scienceClass);
        student4.assignClass(electricalClass);
        // mathClass.assignStudent(student4);
        // scienceClass.assignStudent(student4);

        em.persist(student1); // with cascading effect all of these should get
                              // persisted.

        em.persist(student2);
        em.persist(student3);
        em.persist(student4);

        em.clear();
        StudentMTM result = em.find(StudentMTM.class, 103l);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getFirstName(), "shaheed");

        Assert.assertEquals(3, result.getClasses().size());

        Iterator<ClassMTM> classIter = result.getClasses().iterator();
        while (classIter.hasNext())
        {
            ClassMTM topicClass = classIter.next();
            if (topicClass.getTopic().equals("electrical"))
            {
                Assert.assertEquals(1, topicClass.getStudents().size());
            }
            else if (topicClass.getTopic().equals("maths"))
            {
                Assert.assertEquals(4, topicClass.getStudents().size());
            }
            else if (topicClass.getTopic().equals("science"))
            {
                Assert.assertEquals(3, topicClass.getStudents().size());
            }
            else
            {
                Assert.fail();
            }
        }

        em.remove(student1); // with cascading effect all of these should get
        // persisted.

        em.remove(student2);
        em.remove(student3);
        em.remove(student4);
        
        em.clear();
        
        result = em.find(StudentMTM.class, 103l);
        Assert.assertNull(result);
        em.clear();
        result = em.find(StudentMTM.class, 102l);
        Assert.assertNull(result);
        em.clear();
        result = em.find(StudentMTM.class, 101l);
        Assert.assertNull(result);
        em.clear();
        result = em.find(StudentMTM.class, 100l);
        Assert.assertNull(result);
    }

    @After
    public void tearDown()
    {
        if (emf != null)
        {
            emf.close();
        }
        if (em != null)
        {
            em.close();
        }
        CassandraCli.dropKeySpace("KunderaExamples");
    }

    /**
     * Create schema.
     */
    private void createSchema()
    {
        String createStudentTableQuery = "create table \"Student\"(\"key\" bigint PRIMARY KEY,fname text, lname text)";
        String createClassTableQuery = "create table \"ClassMTM\"(\"key\" bigint PRIMARY KEY,topic text, \"classNo\" int)";
        String createMappingTableQuery = "create table \"STUDENT_CLASS\"(key text PRIMARY KEY,\"STUDENT_ID\" bigint , \"CLASS_ID\" bigint)";
        String classIndexQuery = "create index on \"STUDENT_CLASS\"(\"CLASS_ID\")";
        String studentIndexQuery = "create index on \"STUDENT_CLASS\"(\"STUDENT_ID\")";
        CassandraCli.executeCqlQuery(createStudentTableQuery, "KunderaExamples");
        CassandraCli.executeCqlQuery(createClassTableQuery, "KunderaExamples");
        CassandraCli.executeCqlQuery(createMappingTableQuery, "KunderaExamples");

        CassandraCli.executeCqlQuery(classIndexQuery, "KunderaExamples");
        CassandraCli.executeCqlQuery(studentIndexQuery, "KunderaExamples");
    }

}
