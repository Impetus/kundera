/**
 * 
 */
package com.impetus.client.hbase.crud.datatypes;

import java.io.IOException;
import java.util.List;

import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.junits.HBaseCli;

/**
 * The Class Student test case for HBase.
 * 
 * @author Kuldeep.mishra
 */
public class StudentHbaseTest extends StudentBase<StudentHbase>
{
    private String persistenceUnit = "hbaseTest";

    private HBaseCli cli;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        setupInternal(persistenceUnit);
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
        teardownInternal(persistenceUnit);

    }

    @Test
    public void dummyTest(){
        //TODO HBase embedded server , connection refused  
    }
    
//    @Test
    public void executeTests()
    {
        onInsert();
        onMerge();
    }

    /**
     * Test method for.
     * 
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     *             {@link com.impetus.kundera.examples.student.StudentDao#saveStudent(com.impetus.kundera.examples.crud.datatype.entities.StudentHbase)}
     *             .
     */

    public void onInsert()
    {
        try
        {
            onInsert(new StudentHbase());
        }
        catch (InstantiationException e)
        {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        catch (IllegalAccessException e)
        {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        // find by id.
        StudentEntityDef s = em.find(StudentHbase.class, studentId1);
        assertOnDataTypes((StudentHbase) s);

        // // find by name.
        assertFindByName(em, "StudentHbase", StudentHbase.class, "Amresh", "studentName");

        // find by name and age.
        assertFindByNameAndAge(em, "StudentHbase", StudentHbase.class, "Amresh", "10", "studentName");

        // find by name, age clause
        assertFindByNameAndAgeGTAndLT(em, "StudentHbase", StudentHbase.class, "Amresh", "10", "20", "studentName");
        //
        // // find by between clause
        assertFindByNameAndAgeBetween(em, "StudentHbase", StudentHbase.class, "Amresh", "10", "15", "studentName");

        // find by Range.
        assertFindByRange(em, "StudentHbase", StudentHbase.class, "12345677", "12345678", "studentId");

        // find by without where clause.
        assertFindWithoutWhereClause(em, "StudentHbase", StudentHbase.class);
    }

    /**
     * On merge.
     */
    public void onMerge()
    {
        em.persist(prepareData((Long) studentId1, 78575785897L, "Amresh", true, 10, 'C', (byte) 5, (short) 8,
                (float) 69.6, 163.76765654, enrolmentDate, enrolmentTime, joiningDateAndTime, new Integer(3), new Long(
                        978423946455l), 135434.89, newSqlDate, sqlTime, sqlTimestamp, bigDecimal, bigInteger, calendar,
                new StudentHbase()));
        StudentHbase s = em.find(StudentHbase.class, studentId1);
        Assert.assertNotNull(s);
        Assert.assertEquals("Amresh", s.getStudentName());
        // modify record.
        s.setStudentName("NewAmresh");
        em.merge(s);
        // emf.close();
        Query q = em.createQuery("Select p from StudentHbase p where p.studentName = NewAmresh");
        List<StudentHbase> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
    }

    @Override
    void startServer() throws InterruptedException
    {
        cli = new HBaseCli();
        // cli.init();
        cli.startCluster();
        // TimeUnit.SECONDS.sleep(10);
    }

    @Override
    void stopServer() throws InterruptedException, MasterNotRunningException, ZooKeeperConnectionException, IOException
    {
        // cli.stopCluster();
        cli.stopCluster("STUDENT");
    }

    @Override
    void createSchema()
    {

        // cli.addColumnFamily("STUDENT", columnFamily)
        // HBaseAdmin admin = cli.utility.getHBaseAdmin();
        // HTableDescriptor desc = new HTableDescriptor("STUDENT");
        //
        // desc.addFamily(new HColumnDescriptor("UNIQUE_ID"));
        // desc.addFamily(new HColumnDescriptor("STUDENT_NAME"));
        // desc.addFamily(new HColumnDescriptor("IS_EXCEPTIONAL"));
        // desc.addFamily(new HColumnDescriptor("AGE"));
        // desc.addFamily(new HColumnDescriptor("SEMESTER"));
        // desc.addFamily(new HColumnDescriptor("DIGITAL_SIGNATURE"));
        // desc.addFamily(new HColumnDescriptor("CGPA"));
        // desc.addFamily(new HColumnDescriptor("PERCENTAGE"));
        // desc.addFamily(new HColumnDescriptor("HEIGHT"));
        // desc.addFamily(new HColumnDescriptor("ENROLMENT_DATE"));
        // desc.addFamily(new HColumnDescriptor("ENROLMENT_TIME"));
        // desc.addFamily(new HColumnDescriptor("JOINING_DATE_TIME"));
        // desc.addFamily(new HColumnDescriptor("YEARS_SPENT"));
        // desc.addFamily(new HColumnDescriptor("ROLL_NUMBER"));
        // desc.addFamily(new HColumnDescriptor("MONTHLY_FEE"));
        // desc.addFamily(new HColumnDescriptor("SQL_DATE"));
        // desc.addFamily(new HColumnDescriptor("SQL_TIMESTAMP"));
        // desc.addFamily(new HColumnDescriptor("SQL_TIME"));
        // desc.addFamily(new HColumnDescriptor("BIG_INT"));
        // desc.addFamily(new HColumnDescriptor("BIG_DECIMAL"));
        // desc.addFamily(new HColumnDescriptor("CALENDAR"));

        // cli.utility.createTable("STUDENT"[], desc.ge;

        byte[][] families = new byte[][] { "UNIQUE_ID".getBytes(), "STUDENT_NAME".getBytes(),
                "IS_EXCEPTIONAL".getBytes(), "AGE".getBytes(), "SEMESTER".getBytes(), "DIGITAL_SIGNATURE".getBytes(),
                "CGPA".getBytes(), "PERCENTAGE".getBytes(), "HEIGHT".getBytes(), "ENROLMENT_DATE".getBytes(),
                "ENROLMENT_TIME".getBytes(), "JOINING_DATE_TIME".getBytes(), "YEARS_SPENT".getBytes(),
                "ROLL_NUMBER".getBytes(), "MONTHLY_FEE".getBytes(), "SQL_DATE".getBytes(), "SQL_TIMESTAMP".getBytes(),
                "SQL_TIME".getBytes(), "SQL_TIMESTAMP".getBytes(), "SQL_TIME".getBytes(), "BIG_INT".getBytes(),
                "BIG_DECIMAL".getBytes(), "CALENDAR".getBytes() };
        cli.createTable("STUDENT".getBytes(), families);
        // cli.addColumnFamily("STUDENT", "UNIQUE_ID");
        // cli.addColumnFamily("STUDENT", "STUDENT_NAME");
        // cli.addColumnFamily("STUDENT", "IS_EXCEPTIONAL");
        // cli.addColumnFamily("STUDENT", "AGE");
        // cli.addColumnFamily("STUDENT", "SEMESTER");
        // cli.addColumnFamily("STUDENT", "DIGITAL_SIGNATURE");
        // cli.addColumnFamily("STUDENT", "CGPA");
        // cli.addColumnFamily("STUDENT", "PERCENTAGE");
        // cli.addColumnFamily("STUDENT", "HEIGHT");
        // cli.addColumnFamily("STUDENT", "ENROLMENT_DATE");
        // cli.addColumnFamily("STUDENT", "ENROLMENT_TIME");
        // cli.addColumnFamily("STUDENT", "JOINING_DATE_TIME");
        // cli.addColumnFamily("STUDENT", "YEARS_SPENT");
        // cli.addColumnFamily("STUDENT", "ROLL_NUMBER");
        // cli.addColumnFamily("STUDENT", "MONTHLY_FEE");
        // cli.addColumnFamily("STUDENT", "SQL_DATE");
        // cli.addColumnFamily("STUDENT", "SQL_TIMESTAMP");
        // cli.addColumnFamily("STUDENT", "SQL_TIME");
        // cli.addColumnFamily("STUDENT", "BIG_INT");
        // cli.addColumnFamily("STUDENT", "BIG_DECIMAL");
        // cli.addColumnFamily("STUDENT", "CALENDAR");
    }

    @Override
    void deleteSchema()
    {
        // TestUtilities.cleanLuceneDirectory("hbaseTest");
    }

}
