/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.crud.compositeType.association;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;

/**
 * @author impadmin
 * 
 */
public class UserOTOPKTest
{
    private String keyspace = "KunderaExamples";

    private EntityManagerFactory emf;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.initClient();
        loadData();
        emf = Persistence.createEntityManagerFactory("thriftClientTest");
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace(keyspace);
        emf.close();
    }

    @Test
    public void testOTOPk()
    {
        EntityManager em = emf.createEntityManager();

        AddressOTOPK addressOTOPK = new AddressOTOPK();
        addressOTOPK.setAddressId("xyz");
        addressOTOPK.setStreet("STTRRREEEETTTTT");

        UserOTOPK userOTOPK = new UserOTOPK();
        userOTOPK.setPersonId("1234");
        userOTOPK.setPersonName("Kuldeep");
        userOTOPK.setAddress(addressOTOPK);

        em.persist(userOTOPK);

        em.clear();

        UserOTOPK otopk = em.find(UserOTOPK.class, "1234");
        Assert.assertNotNull(otopk);
        Assert.assertNotNull(otopk.getAddress());
        Assert.assertEquals("Kuldeep", otopk.getPersonName());
        Assert.assertEquals("1234", otopk.getAddress().getPersonId());
        Assert.assertEquals("xyz", otopk.getAddress().getAddressId());
        Assert.assertEquals("STTRRREEEETTTTT", otopk.getAddress().getStreet());

        em.clear();

        Query q = em.createQuery("Select u from UserOTOPK u");
        List<UserOTOPK> users = q.getResultList();
        Assert.assertNotNull(users);
        Assert.assertEquals(1, users.size());
        Assert.assertNotNull(users.get(0));
        Assert.assertNotNull(users.get(0).getAddress());
        Assert.assertEquals("STTRRREEEETTTTT", users.get(0).getAddress().getStreet());
        em.close();

    }

    /**
     * Loads data.
     * 
     * @throws InvalidRequestException
     * @throws SchemaDisagreementException
     * @throws TException
     */
    private void loadData() throws InvalidRequestException, SchemaDisagreementException, TException
    {
        List<CfDef> cfDefs = new ArrayList<CfDef>();

        CfDef user = new CfDef(keyspace, "UserOTOPK");
        user.setKey_validation_class("UTF8Type");
        user.setDefault_validation_class("UTF8Type");
        user.setComparator_type("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("PERSON_NAME".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        user.addToColumn_metadata(columnDef);

        CfDef address = new CfDef(keyspace, "AddressOTOPK");
        address.setKey_validation_class("UTF8Type");
        address.setDefault_validation_class("UTF8Type");
        address.setComparator_type("UTF8Type");
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("ADDRESS_ID".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("STREET".getBytes()), "UTF8Type");
        columnDef2.index_type = IndexType.KEYS;

        address.addToColumn_metadata(columnDef1);
        address.addToColumn_metadata(columnDef2);

        cfDefs.add(user);
        cfDefs.add(address);
        KsDef ksDef = null;
        try
        {
            CassandraCli.initClient();
            ksDef = CassandraCli.client.describe_keyspace(keyspace);
            CassandraCli.client.set_keyspace(keyspace);

            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {
                if (cfDef1.getName().equalsIgnoreCase("UserOTOPK"))
                {
                    CassandraCli.client.system_drop_column_family("UserOTOPK");
                }
                if (cfDef1.getName().equalsIgnoreCase("AddressOTOPK"))
                {
                    CassandraCli.client.system_drop_column_family("AddressOTOPK");
                }
            }
            CassandraCli.client.system_add_column_family(user);
            CassandraCli.client.system_add_column_family(address);
        }
        catch (NotFoundException e)
        {

            ksDef = new org.apache.cassandra.thrift.KsDef(keyspace, "org.apache.cassandra.locator.SimpleStrategy",
                    cfDefs);

            if (ksDef.strategy_options == null)
            {
                ksDef.strategy_options = new LinkedHashMap<String, String>();
            }
            ksDef.strategy_options.put("replication_factor", "1");
            CassandraCli.client.system_add_keyspace(ksDef);

            CassandraCli.client.set_keyspace(keyspace);
        }
        catch (TException e)
        {
            
        }
        catch (InvalidRequestException e)
        {
            
        }
        catch (SchemaDisagreementException e)
        {
            
        }
    }
}
