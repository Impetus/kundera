/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.client.kudu.crud.datatypes;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.kudu.crud.datatypes.entities.StudentKuduInt;

import junit.framework.Assert;

/**
 * The Class StudentKuduIntTest.
 * 
 * @author Devender Yadav
 */
public class StudentKuduIntTest extends KuduBase
{

    /** The Constant KUDU_PU. */
    private static final String KUDU_PU = "dataTypeTest";

    /** The emf. */
    private static EntityManagerFactory emf;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(KUDU_PU);
    }

    /**
     * Tear down after class.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        emf.close();
        emf = null;

    }

    /**
     * Test execute use same em.
     */
    @Test
    public void testExecuteUseSameEm()
    {
        persistStudents();
        testFindById(true);
        testMerge(true);
        testFindByQuery(true);
        testDelete(true);
    }

    /**
     * Test execute.
     */
    @Test
    public void testExecute()
    {
        persistStudents();
        testFindById(false);
        testMerge(false);
        testFindByQuery(false);
        testDelete(false);
    }

    /**
     * Persist students.
     */
    public void persistStudents()
    {
        EntityManager em = emf.createEntityManager();

        // Insert max value of int
        StudentKuduInt studentMax = new StudentKuduInt();
        studentMax.setAge((Short) getMaxValue(short.class));
        studentMax.setId((Integer) getMaxValue(int.class));
        studentMax.setName((String) getMaxValue(String.class));
        em.persist(studentMax);

        // Insert random value of int
        StudentKuduInt student = new StudentKuduInt();
        student.setAge((Short) getRandomValue(short.class));
        student.setId((Integer) getRandomValue(int.class));
        student.setName((String) getRandomValue(String.class));
        em.persist(student);

        // Insert min value of int
        StudentKuduInt studentMin = new StudentKuduInt();
        studentMin.setAge((Short) getPartialValue(short.class));
        studentMin.setId((Integer) getMinValue(int.class));
        studentMin.setName((String) getMinValue(String.class));
        em.persist(studentMin);

        em.close();
    }

    /**
     * Test find by id.
     * 
     * @param useSameEm
     *            the use same em
     */
    public void testFindById(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();

        StudentKuduInt studentMax = em.find(StudentKuduInt.class, getMaxValue(int.class));
        Assert.assertNotNull(studentMax);
        Assert.assertEquals(getMaxValue(short.class), studentMax.getAge());
        Assert.assertEquals(getMaxValue(String.class), studentMax.getName());

        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentKuduInt studentMin = em.find(StudentKuduInt.class, getMinValue(int.class));
        Assert.assertEquals(getPartialValue(short.class), studentMin.getAge());
        Assert.assertEquals(getMinValue(String.class), studentMin.getName());

        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentKuduInt student = em.find(StudentKuduInt.class, getRandomValue(int.class));
        Assert.assertNotNull(student);
        Assert.assertEquals(getRandomValue(short.class), student.getAge());
        Assert.assertEquals(getRandomValue(String.class), student.getName());
        em.close();
    }

    /**
     * Test merge.
     * 
     * @param useSameEm
     *            the use same em
     */
    public void testMerge(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();
        StudentKuduInt student = em.find(StudentKuduInt.class, getMaxValue(int.class));
        Assert.assertNotNull(student);
        Assert.assertEquals(getMaxValue(short.class), student.getAge());
        Assert.assertEquals(getMaxValue(String.class), student.getName());

        student.setName("Kuldeep");
        em.merge(student);
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentKuduInt newStudent = em.find(StudentKuduInt.class, getMaxValue(int.class));
        Assert.assertNotNull(newStudent);
        Assert.assertEquals(getMaxValue(short.class), newStudent.getAge());
        Assert.assertEquals("Kuldeep", newStudent.getName());
    }

    /**
     * Test find by query.
     * 
     * @param useSameEm
     *            the use same em
     */
    public void testFindByQuery(boolean useSameEm)
    {
        findAllQuery();
        findByName();
        findByAge();
        // findByNameAndAgeGTAndLT();
        findByNameAndAgeGTEQAndLTEQ();
        // findByNameAndAgeGTAndLTEQ();
        // findByAgeAndNameGTAndLT();
    }

    /**
     * Find by age and name gt and lt.
     */
    private void findByAgeAndNameGTAndLT()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentKuduInt> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentKuduInt s where s.age = " + getPartialValue(short.class)
                + " and s.name > 'Amresh' and s.name <= '" + getMaxValue(String.class) + "'";
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentKuduInt student : students)
        {
            Assert.assertEquals(getMinValue(int.class), student.getId());
            Assert.assertEquals(getPartialValue(short.class), student.getAge());
            Assert.assertEquals(getMinValue(String.class), student.getName());
            count++;

        }
        Assert.assertEquals(1, count);
        em.close();

    }

    /**
     * Find by name and age gt and lteq.
     */
    private void findByNameAndAgeGTAndLTEQ()
    {

        EntityManager em;
        String query;
        Query q;
        List<StudentKuduInt> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentKuduInt s where s.name = 'Kuldeep' and s.age > " + getPartialValue(short.class)
                + " and s.age <= " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentKuduInt student : students)
        {
            Assert.assertEquals(getMaxValue(int.class), student.getId());
            Assert.assertEquals(getMaxValue(short.class), student.getAge());
            Assert.assertEquals("Kuldeep", student.getName());
            count++;
        }
        Assert.assertEquals(1, count);
        em.close();
    }

    /**
     * Test delete.
     * 
     * @param useSameEm
     *            the use same em
     */
    public void testDelete(boolean useSameEm)
    {
        EntityManager em = emf.createEntityManager();

        StudentKuduInt studentMax = em.find(StudentKuduInt.class, getMaxValue(int.class));
        Assert.assertNotNull(studentMax);
        Assert.assertEquals(getMaxValue(short.class), studentMax.getAge());
        Assert.assertEquals("Kuldeep", studentMax.getName());
        em.remove(studentMax);
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        studentMax = em.find(StudentKuduInt.class, getMaxValue(int.class));
        Assert.assertNull(studentMax);
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }

        StudentKuduInt studentMin = em.find(StudentKuduInt.class, getMinValue(int.class));
        Assert.assertNotNull(studentMin);
        Assert.assertEquals(getPartialValue(short.class), studentMin.getAge());
        Assert.assertEquals("Kuldeep", studentMin.getName());
        em.remove(studentMin);
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        studentMin = em.find(StudentKuduInt.class, getMinValue(int.class));
        Assert.assertNull(studentMin);
    }

    /**
     * Find by name and age gt and lt.
     */
    private void findByNameAndAgeGTAndLT()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentKuduInt> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentKuduInt s where s.name = 'Amresh' and s.age > " + getPartialValue(short.class)
                + " and s.age < " + getMaxValue(short.class);
        // query =
        // "Select s From StudentKuduInt s where s.name = Amresh and s.age > ?1
        // and s.age < ?2";
        q = em.createQuery(query);
        // q.setParameter(1, getPartialValue(short.class));
        // q.setParameter(2, getMaxValue(short.class));
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentKuduInt student : students)
        {
            Assert.assertEquals(getRandomValue(int.class), student.getId());
            Assert.assertEquals(getRandomValue(short.class), student.getAge());
            Assert.assertEquals(getRandomValue(String.class), student.getName());
            count++;

        }
        Assert.assertEquals(1, count);
        em.close();

    }

    /**
     * Find by name and age gteq and lteq.
     */
    private void findByNameAndAgeGTEQAndLTEQ()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentKuduInt> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentKuduInt s where s.name = 'Kuldeep' and s.age >= " + getPartialValue(short.class)
                + " and s.age <= " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        count = 0;
        for (StudentKuduInt student : students)
        {
            if (student.getId() == ((Integer) getMaxValue(int.class)).intValue())
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(getMinValue(int.class), student.getId());
                Assert.assertEquals(getPartialValue(short.class), student.getAge());
                Assert.assertEquals(getMinValue(String.class), student.getName());
                count++;
            }

        }
        Assert.assertEquals(2, count);
        em.close();

    }

    /**
     * Find by age.
     */
    private void findByAge()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentKuduInt> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentKuduInt s where s.age = " + getRandomValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentKuduInt student : students)
        {
            Assert.assertEquals(getRandomValue(int.class), student.getId());
            Assert.assertEquals(getRandomValue(short.class), student.getAge());
            Assert.assertEquals(getRandomValue(String.class), student.getName());
            count++;
        }
        Assert.assertEquals(1, count);
        em.close();
    }

    /**
     * Find by name.
     */
    private void findByName()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentKuduInt> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentKuduInt s where s.name = 'Kuldeep'";
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        count = 0;
        for (StudentKuduInt student : students)
        {
            if (student.getId() == ((Integer) getMaxValue(int.class)).intValue())
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(getMinValue(int.class), student.getId());
                Assert.assertEquals(getPartialValue(short.class), student.getAge());
                Assert.assertEquals(getMinValue(String.class), student.getName());
                count++;
            }
        }
        Assert.assertEquals(2, count);
        em.close();
    }

    /**
     * Find all query.
     */
    private void findAllQuery()
    {
        EntityManager em = emf.createEntityManager();
        // Selet all query.
        String query = "Select s From StudentKuduInt s ";
        Query q = em.createQuery(query);
        List<StudentKuduInt> students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(3, students.size());
        int count = 0;
        for (StudentKuduInt student : students)
        {
            if (student.getId() == ((Integer) getMaxValue(int.class)).intValue())
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else if (student.getId() == ((Integer) getMinValue(int.class)).intValue())
            {
                Assert.assertEquals(getPartialValue(short.class), student.getAge());
                Assert.assertEquals(getMinValue(String.class), student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(getRandomValue(int.class), student.getId());
                Assert.assertEquals(getRandomValue(short.class), student.getAge());
                Assert.assertEquals(getRandomValue(String.class), student.getName());
                count++;
            }
        }
        Assert.assertEquals(3, count);
        em.close();
    }

}
