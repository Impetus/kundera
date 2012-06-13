/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

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
import com.impetus.kundera.metadata.model.KunderaMetadata;

/**
 * Test case to perform simple SelfAssociation.
 * 
 * @author vivek.mishra
 * 
 *         Run this script to create column family in cassandra with indexes.
 *         create column family PERSON with comparator=UTF8Type and
 *         column_metadata=[{column_name: PERSON_NAME,
 *         validation_class:UTF8Type, index_type: KEYS}, {column_name:
 *         AGE,validation_class:IntegerType, index_type: KEYS}, {column_name:
 *         MANAGER_ID,validation_class:UTF8Type, index_type: KEYS}];
 * 
 */
public class PersonAssociationTest extends BaseTest
{

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    private CassandraCli cassandraCli;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        cassandraCli = new CassandraCli();
        cassandraCli.cassandraSetUp();
        loadData();
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        emf = Persistence.createEntityManagerFactory("secIdxCassandraTest");
        em = emf.createEntityManager();
    }

    /**
     * On crud.
     */
    @Test
    public void onInsert()
    {

        Employee emp1 = new Employee();
        emp1.setPersonId("1_e");
        emp1.setPersonName("emp");

        Employee emp2 = new Employee();
        emp2.setPersonId("2_e");
        emp2.setPersonName("emp2");

        Employee manager = new Employee();
        manager.setPersonId("2_m");
        manager.setPersonName("mgr");
        emp1.setManager(manager);

        // set manager for employee2
        emp2.setManager(manager);
        // set manager for employee1
        emp1.setManager(manager);

        Set<Employee> ems = new java.util.HashSet<Employee>();
        ems.add(emp2);
        ems.add(emp1);

        // set employees for manager.
        manager.setEmployees(ems);

        em.persist(manager);
        em.persist(emp1);
        em.persist(emp2);

        Employee emp = em.find(Employee.class, "1_e");

        Assert.assertNotNull(emp);
        Assert.assertEquals("2_m", emp.getManager().getPersonId());
        Assert.assertNull(emp.getEmployees());
        Assert.assertNotNull(emp.getManager().getEmployees());
        Assert.assertFalse(emp.getManager().getEmployees().isEmpty());
        Assert.assertEquals(2, emp.getManager().getEmployees().size());
        Iterator<Employee> iter = emp.getManager().getEmployees().iterator();
        String firstEmployee = iter.next().getPersonId();
        String secEmployee = iter.next().getPersonId();
        Assert.assertNotSame(firstEmployee, secEmployee);
        // Assert.assertEquals("2_e",
        // emp.getEmployees().iterator().next().getPersonId());
    }

    /**
     * Load cassandra specific data.
     * 
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
        CfDef user_Def = new CfDef();
        user_Def.name = "PERSON";
        user_Def.keyspace = "KunderaExamples";
        user_Def.setComparator_type("UTF8Type");
        user_Def.setDefault_validation_class("UTF8Type");
        ColumnDef personName = new ColumnDef(ByteBuffer.wrap("PERSON_NAME".getBytes()), "UTF8Type");
        personName.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(personName);
        ColumnDef age = new ColumnDef(ByteBuffer.wrap("AGE".getBytes()), "IntegerType");
        age.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(age);

        ColumnDef reference_key = new ColumnDef(ByteBuffer.wrap("MANAGER_ID".getBytes()), "UTF8Type");
        reference_key.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(reference_key);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace("KunderaExamples");
            CassandraCli.client.set_keyspace("KunderaExamples");

            CassandraCli.client.system_add_column_family(user_Def);

        }
        catch (NotFoundException e)
        {

            ksDef = new KsDef("KunderaExamples", "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
            ksDef.setReplication_factor(1);
            CassandraCli.client.system_add_keyspace(ksDef);
        }

        CassandraCli.client.set_keyspace("KunderaExamples");

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
        emf.close();
        cassandraCli.dropKeySpace("KunderaExamples");
        /*
         * Delete is working, but as row keys are not deleted from cassandra, so
         * resulting in issue while reading back. // Delete
         * em.remove(em.find(Person.class, "1"));
         * em.remove(em.find(Person.class, "2"));
         * em.remove(em.find(Person.class, "3")); em.close(); emf.close(); em =
         * null; emf = null;
         */
    }
}
