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

import com.impetus.client.kudu.crud.datatypes.entities.StudentKuduBytePrimitive;

import junit.framework.Assert;

/**
 * The Class StudentKuduBytePrimitiveTest.
 * 
 * @author Devender Yadav
 */
public class StudentKuduBytePrimitiveTest extends KuduBase
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

        // Insert max value of byte
        StudentKuduBytePrimitive studentMax = new StudentKuduBytePrimitive();
        studentMax.setAge((Short) getMaxValue(short.class));
        studentMax.setId((Byte) getMaxValue(byte.class));
        studentMax.setName((String) getMaxValue(String.class));
        em.persist(studentMax);

        // Insert min value of byte
        StudentKuduBytePrimitive studentMin = new StudentKuduBytePrimitive();
        studentMin.setAge((Short) getPartialValue(short.class));
        studentMin.setId((Byte) getMinValue(byte.class));
        studentMin.setName((String) getMinValue(String.class));
        em.persist(studentMin);

        // Insert random value of byte
        StudentKuduBytePrimitive student = new StudentKuduBytePrimitive();
        student.setAge((Short) getRandomValue(short.class));
        student.setId((Byte) getRandomValue(byte.class));
        student.setName((String) getRandomValue(String.class));
        em.persist(student);
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

        StudentKuduBytePrimitive studentMax = em.find(StudentKuduBytePrimitive.class, getMaxValue(byte.class));
        Assert.assertNotNull(studentMax);
        Assert.assertEquals(getMaxValue(short.class), studentMax.getAge());
        Assert.assertEquals(getMaxValue(String.class), studentMax.getName());

        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentKuduBytePrimitive studentMin = em.find(StudentKuduBytePrimitive.class, getMinValue(byte.class));
        Assert.assertNotNull(studentMin);
        Assert.assertEquals(getPartialValue(short.class), studentMin.getAge());
        Assert.assertEquals(getMinValue(String.class), studentMin.getName());

        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        StudentKuduBytePrimitive student = em.find(StudentKuduBytePrimitive.class, getRandomValue(byte.class));
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
        StudentKuduBytePrimitive student = em.find(StudentKuduBytePrimitive.class, getMaxValue(byte.class));
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
        StudentKuduBytePrimitive newStudent = em.find(StudentKuduBytePrimitive.class, getMaxValue(byte.class));
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
        List<StudentKuduBytePrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentKuduBytePrimitive s where s.age = " + getPartialValue(short.class)
                + " and s.name > 'Amresh' and s.name <= '" + getMaxValue(String.class) + "'";
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentKuduBytePrimitive student : students)
        {
            Assert.assertEquals(getMinValue(byte.class), student.getId());
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
        List<StudentKuduBytePrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentKuduBytePrimitive s where s.name = 'Kuldeep' and s.age > "
                + getPartialValue(short.class) + " and s.age <= " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentKuduBytePrimitive student : students)
        {
            Assert.assertEquals(getMaxValue(byte.class), student.getId());
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

        StudentKuduBytePrimitive studentMax = em.find(StudentKuduBytePrimitive.class, getMaxValue(byte.class));
        Assert.assertNotNull(studentMax);
        Assert.assertEquals(getMaxValue(short.class), studentMax.getAge());
        Assert.assertEquals("Kuldeep", studentMax.getName());
        em.remove(studentMax);
        if (!useSameEm)
        {
            em.close();
            em = emf.createEntityManager();
        }
        studentMax = em.find(StudentKuduBytePrimitive.class, getMaxValue(byte.class));
        Assert.assertNull(studentMax);
        em.close();
    }

    /**
     * Find by name and age gt and lt.
     */
    private void findByNameAndAgeGTAndLT()
    {
        EntityManager em;
        String query;
        Query q;
        List<StudentKuduBytePrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentKuduBytePrimitive s where s.name = 'Amresh' and s.age > "
                + getPartialValue(short.class) + " and s.age < " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentKuduBytePrimitive student : students)
        {
            Assert.assertEquals(getRandomValue(byte.class), student.getId());
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
        List<StudentKuduBytePrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentKuduBytePrimitive s where s.name = 'Kuldeep' and s.age >= "
                + getPartialValue(short.class) + " and s.age <= " + getMaxValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        count = 0;
        for (StudentKuduBytePrimitive student : students)
        {
            if (student.getId() == ((Byte) getMaxValue(byte.class)).byteValue())
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(getMinValue(byte.class), student.getId());
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
        List<StudentKuduBytePrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentKuduBytePrimitive s where s.age = " + getRandomValue(short.class);
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(1, students.size());
        count = 0;
        for (StudentKuduBytePrimitive student : students)
        {
            Assert.assertEquals(getRandomValue(byte.class), student.getId());
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
        List<StudentKuduBytePrimitive> students;
        int count;
        em = emf.createEntityManager();
        query = "Select s From StudentKuduBytePrimitive s where s.name = 'Kuldeep'";
        q = em.createQuery(query);
        students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(2, students.size());
        count = 0;
        for (StudentKuduBytePrimitive student : students)
        {
            if (student.getId() == ((Byte) getMaxValue(byte.class)).byteValue())
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(getMinValue(byte.class), student.getId());
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
        String query = "Select s From StudentKuduBytePrimitive s ";
        Query q = em.createQuery(query);
        List<StudentKuduBytePrimitive> students = q.getResultList();
        Assert.assertNotNull(students);
        Assert.assertEquals(3, students.size());
        int count = 0;
        for (StudentKuduBytePrimitive student : students)
        {
            if (student.getId() == ((Byte) getMaxValue(byte.class)).byteValue())
            {
                Assert.assertEquals(getMaxValue(short.class), student.getAge());
                Assert.assertEquals("Kuldeep", student.getName());
                count++;
            }
            else if (student.getId() == ((Byte) getMinValue(byte.class)).byteValue())
            {
                Assert.assertEquals(getPartialValue(short.class), student.getAge());
                Assert.assertEquals(getMinValue(String.class), student.getName());
                count++;
            }
            else
            {
                Assert.assertEquals(getRandomValue(byte.class), student.getId());
                Assert.assertEquals(getRandomValue(short.class), student.getAge());
                Assert.assertEquals(getRandomValue(String.class), student.getName());
                count++;
            }
        }
        Assert.assertEquals(3, count);
        em.close();
    }

}
