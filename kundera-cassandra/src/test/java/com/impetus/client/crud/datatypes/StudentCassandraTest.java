/**
 * 
 */
package com.impetus.client.crud.datatypes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;

/**
 * The Class StudentDaoTest. script to create Cassandra column family for this
 * test case:
 * 
 * @see create column family STUDENT with comparator=AsciiType and
 *      key_validation_class=LongType and column_metadata=[{column_name:
 *      STUDENT_NAME, validation_class:UTF8Type, index_type: KEYS},
 *      {column_name: AGE, validation_class:IntegerType, index_type: KEYS},
 *      {column_name: UNIQUE_ID, validation_class:IntegerType, index_type:
 *      KEYS}, {column_name: IS_EXCEPTIONAL, validation_class:IntegerType,
 *      index_type: KEYS}, {column_name: SEMESTER, validation_class:IntegerType,
 *      index_type: KEYS}, {column_name: DIGITAL_SIGNATURE,
 *      validation_class:IntegerType, index_type: KEYS}, {column_name: CGPA,
 *      validation_class:IntegerType, index_type: KEYS}, {column_name:
 *      PERCENTAGE, validation_class:IntegerType, index_type: KEYS},
 *      {column_name: HEIGHT, validation_class:IntegerType, index_type: KEYS},
 *      {column_name: ENROLMENT_DATE, validation_class:IntegerType, index_type:
 *      KEYS}, {column_name: ENROLMENT_TIME, validation_class:IntegerType,
 *      index_type: KEYS}, {column_name: JOINING_DATE_TIME,
 *      validation_class:IntegerType, index_type: KEYS}, {column_name:
 *      YEARS_SPENT, validation_class:IntegerType, index_type: KEYS},
 *      {column_name: ROLL_NUMBER, validation_class:IntegerType, index_type:
 *      KEYS}, {column_name: MONTHLY_FEE, validation_class:IntegerType,
 *      index_type: KEYS}, {column_name: SQL_DATE, validation_class:IntegerType,
 *      index_type: KEYS}, {column_name: SQL_TIMESTAMP,
 *      validation_class:IntegerType, index_type: KEYS}, {column_name: SQL_TIME,
 *      validation_class:IntegerType, index_type: KEYS}, {column_name: BIG_INT,
 *      validation_class:IntegerType, index_type: KEYS}, {column_name:
 *      BIG_DECIMAL, validation_class:IntegerType, index_type: KEYS},
 *      {column_name: CALENDAR, validation_class:UTF8Type, index_type: KEYS}];
 * @author Vivek Mishra
 */
public class StudentCassandraTest extends StudentBase<StudentCassandra>
{
    String persistenceUnit = "secIdxCassandraTest";
    

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
    
    @SuppressWarnings("deprecation")
    @Test
    public void executeTests() {
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
     *             {@link com.impetus.kundera.examples.student.StudentDao#saveStudent(com.impetus.kundera.examples.crud.datatype.entities.StudentCassandra)}
     *             .
     */
    
    public void onInsert() 
    {
        try
        {
        onInsert(new StudentCassandra());

        // find by id.
        StudentEntityDef s = em.find(StudentCassandra.class, studentId1);
        assertOnDataTypes((StudentCassandra) s);

        // // find by name.
        assertFindByName(em, "StudentCassandra", StudentCassandra.class, "Amresh", "studentName");

        // find by Id 
//        assertFindByGTId(em, "StudentCassandra", StudentCassandra.class, "12345677", "studentId");
        
        // find by name and age.
        assertFindByNameAndAge(em, "StudentCassandra", StudentCassandra.class, "Amresh", "10", "studentName");

        // find by name, age clause
        assertFindByNameAndAgeGTAndLT(em, "StudentCassandra", StudentCassandra.class, "Amresh", "10", "20",
                "studentName");
        //
        // // find by between clause
        assertFindByNameAndAgeBetween(em, "StudentCassandra", StudentCassandra.class, "Amresh", "10", "15",
                "studentName");

        // find by Range.
        assertFindByRange(em, "StudentCassandra", StudentCassandra.class, "12345677", "12345678", "studentId");

        // find by without where clause.
        assertFindWithoutWhereClause(em, "StudentCassandra", StudentCassandra.class);
        }catch(Exception e)
        {
            e.printStackTrace();
            Assert.fail("Failure onInsert test");
        }
    }

    /**
     * On merge.
     */
    public void onMerge()
    {
         try
         {
        em.persist(prepareData((Long) studentId1, 78575785897L, "Amresh", true, 10, 'C', (byte) 5, (short) 8,
                (float) 69.6, 163.76765654, enrolmentDate, enrolmentTime, joiningDateAndTime, new Integer(3), new Long(
                        978423946455l), 135434.89, newSqlDate, sqlTime, sqlTimestamp, bigDecimal, bigInteger, calendar,
                new StudentCassandra()));
        StudentCassandra s = em.find(StudentCassandra.class, studentId1);
        Assert.assertNotNull(s);
        Assert.assertEquals("Amresh", s.getStudentName());
        // modify record.
        s.setStudentName("NewAmresh");
        em.merge(s);
        // emf.close();
//        assertOnMerge(em, "StudentCassandra", StudentCassandra.class, "Amresh", "NewAmresh", "STUDENT_NAME");
        Query q = em.createQuery("Select p from StudentCassandra p where p.studentName = NewAmresh");
        List<StudentCassandra> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
         }catch(Exception e)
         {
             Assert.fail("Failure onMerge test");
         }

    }

    /**
     * Loads cassandra specific data.
     * @throws TException
     * @throws InvalidRequestException
     * @throws UnavailableException
     * @throws TimedOutException
     * @throws SchemaDisagreementException
     */
    private void loadData() throws TException, InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException
    {

        KsDef ksDef = null;

        CfDef cfDef = new CfDef();
        cfDef.name = "STUDENT";
        cfDef.keyspace = "KunderaExamples";

        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("UniqueId".getBytes()), "IntegerType");
        columnDef2.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef2);
        ColumnDef columnDef3 = new ColumnDef(ByteBuffer.wrap("STUDENT_NAME".getBytes()), "UTF8Type");
        columnDef3.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef3);
        ColumnDef columnDef4 = new ColumnDef(ByteBuffer.wrap("IS_EXCEPTIONAL".getBytes()), "IntegerType");
        columnDef4.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef4);

        ColumnDef columnDef5 = new ColumnDef(ByteBuffer.wrap("AGE".getBytes()), "IntegerType");
        columnDef5.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef5);

        ColumnDef columnDef6 = new ColumnDef(ByteBuffer.wrap("SEMESTER".getBytes()), "IntegerType");
        columnDef6.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef6);

        ColumnDef columnDef7 = new ColumnDef(ByteBuffer.wrap("DIGITAL_SIGNATURE".getBytes()), "IntegerType");
        columnDef7.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef7);

        ColumnDef columnDef8 = new ColumnDef(ByteBuffer.wrap("CGPA".getBytes()), "IntegerType");
        columnDef8.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef8);

        ColumnDef columnDef9 = new ColumnDef(ByteBuffer.wrap("PERCENTAGE".getBytes()), "IntegerType");
        columnDef9.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef9);

        ColumnDef columnDef10 = new ColumnDef(ByteBuffer.wrap("HEIGHT".getBytes()), "IntegerType");
        columnDef10.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef10);

        ColumnDef columnDef11 = new ColumnDef(ByteBuffer.wrap("YEARS_SPENT".getBytes()), "IntegerType");
        columnDef11.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef11);

        ColumnDef columnDef12 = new ColumnDef(ByteBuffer.wrap("ROLL_NUMBER".getBytes()), "IntegerType");
        columnDef12.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef12);

        ColumnDef columnDef13 = new ColumnDef(ByteBuffer.wrap("SQL_DATE".getBytes()), "IntegerType");
        columnDef13.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef13);

        ColumnDef columnDef14 = new ColumnDef(ByteBuffer.wrap("SQL_TIMESTAMP".getBytes()), "IntegerType");
        columnDef14.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef14);

        ColumnDef columnDef15 = new ColumnDef(ByteBuffer.wrap("SQL_TIME".getBytes()), "IntegerType");
        columnDef15.index_type = IndexType.KEYS;

        ColumnDef columnDef16 = new ColumnDef(ByteBuffer.wrap("BIG_INT".getBytes()), "IntegerType");
        columnDef16.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef15);

        ColumnDef columnDef17 = new ColumnDef(ByteBuffer.wrap("BIG_DECIMAL".getBytes()), "IntegerType");
        columnDef17.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef17);

        ColumnDef columnDef18 = new ColumnDef(ByteBuffer.wrap("CALENDAR".getBytes()), "UTF8Type");
        columnDef18.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef18);

        ColumnDef columnDef19 = new ColumnDef(ByteBuffer.wrap("MONTHLY_FEE".getBytes()), "IntegerType");
        columnDef19.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef19);

        ColumnDef columnDef20 = new ColumnDef(ByteBuffer.wrap("ENROLMENT_DATE".getBytes()), "IntegerType");
        columnDef20.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef20);

        ColumnDef columnDef21 = new ColumnDef(ByteBuffer.wrap("ENROLMENT_TIME".getBytes()), "IntegerType");
        columnDef21.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef21);

        ColumnDef columnDef22 = new ColumnDef(ByteBuffer.wrap("JOINING_DATE_TIME".getBytes()), "IntegerType");
        columnDef22.index_type = IndexType.KEYS;
        cfDef.addToColumn_metadata(columnDef22);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(cfDef);
        try
        {
            ksDef = CassandraCli.client.describe_keyspace("KunderaExamples");
            CassandraCli.client.set_keyspace("KunderaExamples");

            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase("PERSONNEL"))
                {

                    CassandraCli.client.system_drop_column_family("PERSONNEL");

                }
            }
            CassandraCli.client.system_add_column_family(cfDef);

        }
        catch (NotFoundException e)
        {

            ksDef = new KsDef("KunderaExamples", "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
            //Set replication factor
            if (ksDef.strategy_options == null) {
                ksDef.strategy_options = new LinkedHashMap<String, String>();
            }
            //Set replication factor, the value MUST be an integer
            ksDef.strategy_options.put("replication_factor", "1");
            CassandraCli.client.system_add_keyspace(ksDef);
        }

        CassandraCli.client.set_keyspace("KunderaExamples");

    }

    @Override
    void startServer()
    {
        try
        {
            CassandraCli.cassandraSetUp();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (TException e)
        {
            e.printStackTrace();
        }
        catch (InvalidRequestException e)
        {
            e.printStackTrace();
        }
        catch (UnavailableException e)
        {
            e.printStackTrace();
        }
        catch (TimedOutException e)
        {
            e.printStackTrace();
        }
        catch (SchemaDisagreementException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    void stopServer()
    {
    }

    @Override
    void createSchema()
    {
        try
        {
            loadData();
        }
        catch (TException e)
        {
            e.printStackTrace();
        }
        catch (InvalidRequestException e)
        {
            e.printStackTrace();
        }
        catch (UnavailableException e)
        {
            e.printStackTrace();
        }
        catch (TimedOutException e)
        {
            e.printStackTrace();
        }
        catch (SchemaDisagreementException e)
        {
            e.printStackTrace();
        } 
    }

    @Override
    void deleteSchema()
    {
        CassandraCli.dropKeySpace("KunderaExamples");
    }
    
    

}