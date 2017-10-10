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
package com.impetus.client.schemamanager;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.thrift.ThriftClientFactory;
import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.ClientResolver;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.configure.schema.SchemaGenerationException;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

/**
 * @author Kuldeep.Kumar
 * 
 */
//TODO: Check for TTransportException
public class CassandraSchemaOperationTest
{
    private Cassandra.Client client;

    /**
     * @throws java.lang.Exception
     */
    //@Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        client = CassandraCli.getClient();
    }

    /**
     * @throws java.lang.Exception
     */
    //@After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace("KunderaCoreExmples");
    }

    //@Test
    public void testCreate() throws NotFoundException, InvalidRequestException, TException,
            UnsupportedEncodingException
    {
        getEntityManagerFactory("create");
        Assert.assertTrue(CassandraCli.keyspaceExist("KunderaCoreExmples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntitySimple", "KunderaCoreExmples"));
        org.apache.cassandra.thrift.KsDef ksDef = new KsDef();
        ksDef = client.describe_keyspace("KunderaCoreExmples");
        Assert.assertEquals(1, ksDef.getCf_defs().size());
        for (org.apache.cassandra.thrift.CfDef cfDef : ksDef.getCf_defs())
        {
            if ("CassandraEntitySimple".equals(cfDef.getName()))
            {
                Assert.assertEquals("CassandraEntitySimple", cfDef.getName());

                Assert.assertEquals("Standard", cfDef.column_type);
                Assert.assertEquals(2, cfDef.getColumn_metadata().size());
                List<String> columns = new ArrayList<String>();
                columns.add("AGE");
                columns.add("PERSON_NAME");
                for (ColumnDef columnDef : cfDef.getColumn_metadata())
                {
                    if (new String(columnDef.getName(), Constants.ENCODING).equals("AGE"))
                    {
                        Assert.assertTrue(columnDef.isSetIndex_type());
                        Assert.assertTrue(columns.contains(new String(columnDef.getName(), Constants.ENCODING)));
                        Assert.assertEquals(IndexType.KEYS, columnDef.index_type);
                        // Assert.assertEquals("age_index",
                        // columnDef.index_name);
                    }
                    else
                    {

                        Assert.assertTrue(columnDef.isSetIndex_type());
                        Assert.assertTrue(columns.contains(new String(columnDef.getName(), Constants.ENCODING)));
                        Assert.assertEquals(IndexType.KEYS, columnDef.index_type);
                        // Assert.assertEquals("name_index",
                        // columnDef.index_name);
                    }
                }
            }
        }
    }

    //@Test
    public void testCreatedrop() throws NotFoundException, InvalidRequestException, TException,
            UnsupportedEncodingException
    {
        getEntityManagerFactory("create-drop");
        Assert.assertTrue(CassandraCli.keyspaceExist("KunderaCoreExmples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntitySimple", "KunderaCoreExmples"));
        org.apache.cassandra.thrift.KsDef ksDef = new KsDef();
        ksDef = client.describe_keyspace("KunderaCoreExmples");
        for (org.apache.cassandra.thrift.CfDef cfDef : ksDef.getCf_defs())
        {
            if ("CassandraEntitySimple".equals(cfDef.getName()))
            {
                Assert.assertEquals("CassandraEntitySimple", cfDef.getName());

                Assert.assertEquals("Standard", cfDef.column_type);
                Assert.assertEquals(2, cfDef.getColumn_metadata().size());

                List<String> columns = new ArrayList<String>();
                columns.add("AGE");
                columns.add("PERSON_NAME");

                for (ColumnDef columnDef : cfDef.getColumn_metadata())
                {
                    if (new String(columnDef.getName(), Constants.ENCODING).equals("AGE"))
                    {
                        Assert.assertTrue(columnDef.isSetIndex_type());
                        Assert.assertTrue(columns.contains(new String(columnDef.getName(), Constants.ENCODING)));
                        Assert.assertEquals(IndexType.KEYS, columnDef.index_type);
                        // Assert.assertEquals("age_index",
                        // columnDef.index_name);
                    }
                    else
                    {
                        Assert.assertTrue(columnDef.isSetIndex_type());
                        Assert.assertTrue(columns.contains(new String(columnDef.getName(), Constants.ENCODING)));
                        Assert.assertEquals(IndexType.KEYS, columnDef.index_type);
                        // Assert.assertEquals("name_index",
                        // columnDef.index_name);
                    }
                }
            }
        }
        ThriftClientFactory clientFactory = (ThriftClientFactory) ClientResolver
                .getClientFactory("CassandraSchemaOperationTest");
        clientFactory.getSchemaManager(null).dropSchema();
        Assert.assertFalse(CassandraCli.keyspaceExist("KunderaCoreExmples"));
    }

    //@Test
    public void testUpdate() throws NotFoundException, InvalidRequestException, TException,
            SchemaDisagreementException, UnsupportedEncodingException
    {
        CassandraCli.createKeySpace("KunderaCoreExmples");
        client.set_keyspace("KunderaCoreExmples");
        org.apache.cassandra.thrift.CfDef cf_def = new org.apache.cassandra.thrift.CfDef("KunderaCoreExmples",
                "CassandraEntitySimple");
        cf_def.column_type = "Standard";
        client.system_add_column_family(cf_def);

        Assert.assertTrue(CassandraCli.keyspaceExist("KunderaCoreExmples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntitySimple", "KunderaCoreExmples"));
        org.apache.cassandra.thrift.KsDef ksDef = new KsDef();
        ksDef = client.describe_keyspace("KunderaCoreExmples");
        Assert.assertEquals(0, ksDef.getCf_defs().get(0).getColumn_metadata().size());

        getEntityManagerFactory("update");
        Assert.assertTrue(CassandraCli.keyspaceExist("KunderaCoreExmples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntitySimple", "KunderaCoreExmples"));
        ksDef = client.describe_keyspace("KunderaCoreExmples");
        for (org.apache.cassandra.thrift.CfDef cfDef : ksDef.getCf_defs())
        {
            if ("CassandraEntitySimple".equals(cfDef.getName()))
            {
                Assert.assertEquals("CassandraEntitySimple", cfDef.getName());

                Assert.assertEquals("Standard", cfDef.getColumn_type());

                int counter = 0;
                for (ColumnDef columnDef : cfDef.getColumn_metadata())
                {
                    if (new String(columnDef.getName(), Constants.ENCODING).equals("AGE"))
                    {
                        Assert.assertTrue(columnDef.isSetIndex_type());
                        Assert.assertNotNull(columnDef.index_name);
                        Assert.assertEquals(IntegerType.class.getName(), columnDef.getValidation_class());
                        // Assert.assertEquals("age_index",
                        // columnDef.index_name);
                        counter++;
                    }
                    else
                    {
                        Assert.assertTrue(columnDef.isSetIndex_type());
                        Assert.assertEquals("PERSON_NAME", new String(columnDef.getName(), Constants.ENCODING));
                        Assert.assertNotNull(columnDef.index_name);
                        Assert.assertEquals(UTF8Type.class.getName(), columnDef.getValidation_class());
                        // Assert.assertEquals("name_index",
                        // columnDef.index_name);
                        counter++;
                    }
                }
                Assert.assertEquals(2, counter);
            }
        }
    }

    //@Test
    public void testUpdateInValid() throws NotFoundException, InvalidRequestException, TException,
            SchemaDisagreementException, UnsupportedEncodingException
    {
        CassandraCli.createKeySpace("KunderaCoreExmples");
        client.set_keyspace("KunderaCoreExmples");
        org.apache.cassandra.thrift.CfDef cf_def = new org.apache.cassandra.thrift.CfDef("KunderaCoreExmples",
                "CassandraEntitySimple");
        cf_def.column_type = "Standard";
        List<ColumnDef> column_metadata = new ArrayList<ColumnDef>();
        ColumnDef def = new ColumnDef();
        def.setName("AGE".getBytes());
        def.setValidation_class("Int32Type");
        def.setIndex_type(IndexType.KEYS);

        ColumnDef def1 = new ColumnDef();
        def1.setName("PERSON_NAME".getBytes());
        def1.setValidation_class("UTF8Type");
        def1.setIndex_type(IndexType.KEYS);

        column_metadata.add(def1);
        column_metadata.add(def);

        cf_def.setColumn_metadata(column_metadata);
        client.system_add_column_family(cf_def);

        Assert.assertTrue(CassandraCli.keyspaceExist("KunderaCoreExmples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntitySimple", "KunderaCoreExmples"));
        org.apache.cassandra.thrift.KsDef ksDef = client.describe_keyspace("KunderaCoreExmples");
        Assert.assertEquals(1, ksDef.getCf_defs().size());
        Assert.assertEquals(2, ksDef.getCf_defs().get(0).getColumn_metadata().size());

        getEntityManagerFactory("update");

        // TODO: Do not delete it.
        Assert.assertTrue(CassandraCli.keyspaceExist("KunderaCoreExmples"));
        Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntitySimple", "KunderaCoreExmples"));
        ksDef = client.describe_keyspace("KunderaCoreExmples");
        for (org.apache.cassandra.thrift.CfDef cfDef : ksDef.getCf_defs())
        {
            if ("CassandraEntitySimple".equals(cfDef.getName()))
            {
                Assert.assertEquals("CassandraEntitySimple", cfDef.getName());

                Assert.assertEquals("Standard", cfDef.getColumn_type());

                int counter = 0;
                for (ColumnDef columnDef : cfDef.getColumn_metadata())
                {
                    if (new String(columnDef.getName(), Constants.ENCODING).equals("AGE"))
                    {
                        Assert.assertTrue(columnDef.isSetIndex_type());
                        Assert.assertNotNull(columnDef.index_name);
                        Assert.assertEquals(IntegerType.class.getName(), columnDef.getValidation_class());
                        // Assert.assertEquals("age_index",
                        // columnDef.index_name);
                        counter++;
                    }
                    else
                    {
                        Assert.assertTrue(columnDef.isSetIndex_type());
                        Assert.assertEquals("PERSON_NAME", new String(columnDef.getName(), Constants.ENCODING));
                        Assert.assertNotNull(columnDef.index_name);
                        Assert.assertEquals(UTF8Type.class.getName(), columnDef.getValidation_class());
                        // Assert.assertEquals("name_index",
                        // columnDef.index_name);
                        counter++;
                    }
                }
                Assert.assertEquals(2, counter);
            }
        }
    }

    //@Test
    public void testValidate()
    {
        try
        {
            CassandraCli.createKeySpace("KunderaCoreExmples");

            client.set_keyspace("KunderaCoreExmples");

            org.apache.cassandra.thrift.CfDef cf_def = new org.apache.cassandra.thrift.CfDef("KunderaCoreExmples",
                    "CassandraEntitySimple");
            cf_def.column_type = "Standard";
            client.system_add_column_family(cf_def);

            org.apache.cassandra.thrift.CfDef actor = new org.apache.cassandra.thrift.CfDef("KunderaCoreExmples",
                    "Actor");
            actor.column_type = "Super";

            client.system_add_column_family(actor);

            getEntityManagerFactory("validate");
            Assert.assertTrue(CassandraCli.keyspaceExist("KunderaCoreExmples"));
            Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntitySimple", "KunderaCoreExmples"));
            org.apache.cassandra.thrift.KsDef ksDef = client.describe_keyspace("KunderaCoreExmples");
            for (org.apache.cassandra.thrift.CfDef cfDef : ksDef.getCf_defs())
            {
                if ("CassandraEntitySimple".equals(cfDef.getName()))
                {
                    Assert.assertEquals("CassandraEntitySimple", cfDef.getName());

                    Assert.assertEquals("Standard", cfDef.column_type);
                    List<String> columns = new ArrayList<String>();
                    columns.add("AGE");
                    columns.add("PERSON_NAME");

                    for (ColumnDef columnDef : cfDef.getColumn_metadata())
                    {
                        Assert.assertFalse(columnDef.isSetIndex_type());
                        Assert.assertTrue(columns.contains(new String(columnDef.getName(), Constants.ENCODING)));
                        Assert.assertNull(columnDef.index_name);
                    }
                }
            }
        }
        catch (SchemaGenerationException e)
        {
            List<String> errors = new ArrayList<String>();
            errors.add("com.impetus.kundera.configure.schema.SchemaGenerationException: Column AGE does not exist in column family CassandraEntitySimple");
            errors.add("com.impetus.kundera.configure.schema.SchemaGenerationException: Column PERSON_NAME does not exist in column family CassandraEntitySimple");
            Assert.assertTrue(errors.contains(e.getMessage()));

        }
        catch (InvalidRequestException e1)
        {
            Assert.fail("failed caused by:" + e1.getMessage());
        }
        catch (TException e1)
        {
            Assert.fail("failed caused by:" + e1.getMessage());
        }
        catch (UnsupportedEncodingException e)
        {
            Assert.fail("failed caused by:" + e.getMessage());
        }
    }

    //@Test
    public void validateInvalidSchema() throws NotFoundException, InvalidRequestException, TException,
            SchemaDisagreementException, UnsupportedEncodingException
    {
        try
        {
            CassandraCli.createKeySpace("KunderaCoreExmples");
            client.set_keyspace("KunderaCoreExmples");
            org.apache.cassandra.thrift.CfDef cf_def = new org.apache.cassandra.thrift.CfDef("KunderaCoreExmples",
                    "CassandraEntitySimple");
            cf_def.column_type = "Standard";
            List<ColumnDef> column_metadata = new ArrayList<ColumnDef>();
            ColumnDef def = new ColumnDef();
            def.setName("AGE".getBytes());
            def.setValidation_class("UTF8Type");
            def.setIndex_type(IndexType.KEYS);

            ColumnDef def1 = new ColumnDef();
            def1.setName("PERSON_NAME".getBytes());
            def1.setValidation_class("BytesType");
            def1.setIndex_type(IndexType.KEYS);

            column_metadata.add(def1);
            column_metadata.add(def);

            cf_def.setColumn_metadata(column_metadata);
            client.system_add_column_family(cf_def);

            org.apache.cassandra.thrift.CfDef actor = new org.apache.cassandra.thrift.CfDef("KunderaCoreExmples",
                    "Actor");
            actor.column_type = "Super";

            client.system_add_column_family(actor);

            Assert.assertTrue(CassandraCli.keyspaceExist("KunderaCoreExmples"));
            Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntitySimple", "KunderaCoreExmples"));
            org.apache.cassandra.thrift.KsDef ksDef = client.describe_keyspace("KunderaCoreExmples");
            Assert.assertEquals(2, ksDef.getCf_defs().size());
            Assert.assertTrue(2 == ksDef.getCf_defs().get(0).getColumn_metadata().size()
                    || 0 == ksDef.getCf_defs().get(0).getColumn_metadata().size());
            Assert.assertTrue(1 == ksDef.getCf_defs().get(1).getColumn_metadata().size()
                    || 1 == ksDef.getCf_defs().get(1).getColumn_metadata().size());

            getEntityManagerFactory("validate");
            Assert.assertTrue(CassandraCli.keyspaceExist("KunderaCassandraExamples"));
            Assert.assertTrue(CassandraCli.columnFamilyExist("CassandraEntitySimple", "KunderaCassandraExamples"));
            ksDef = client.describe_keyspace("KunderaCassandraExamples");
            for (org.apache.cassandra.thrift.CfDef cfDef : ksDef.getCf_defs())
            {
                if ("CassandraEntitySimple".equals(cfDef.getName()))
                {
                    Assert.assertEquals("CassandraEntitySimple", cfDef.getName());

                    Assert.assertEquals("Standard", cfDef.getColumn_type());

                    List<String> columns = new ArrayList<String>();
                    columns.add("AGE");
                    columns.add("PERSON_NAME");
                    for (ColumnDef columnDef : cfDef.getColumn_metadata())
                    {
                        Assert.assertTrue(columnDef.isSetIndex_type());
                        Assert.assertTrue(columns.contains(new String(columnDef.getName(), Constants.ENCODING)));
                        Assert.assertNotNull(columnDef.index_name);
                    }
                }
            }
        }
        catch (SchemaGenerationException sgex)
        {
            List<String> errors = new ArrayList<String>();
            errors.add("com.impetus.kundera.configure.schema.SchemaGenerationException: Column AGE does not exist in column family CassandraEntitySimple");
            errors.add("com.impetus.kundera.configure.schema.SchemaGenerationException: Column PERSON_NAME does not exist in column family CassandraEntitySimple");
            errors.add("com.impetus.kundera.configure.schema.SchemaGenerationException: column family CassandraEntitySimple does not exist in keyspace KunderaCoreExmples");
            Assert.assertTrue(errors.contains(sgex.getMessage().trim()));
        }
    }

    /**
     * Gets the entity manager factory.
     * 
     * @param useLucene
     * @param property
     * 
     * @return the entity manager factory
     */
    private EntityManagerFactoryImpl getEntityManagerFactory(String property)
    {
        Map<String, String> propertyMap = new HashMap<String, String>();
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, property);
        return (EntityManagerFactoryImpl) Persistence.createEntityManagerFactory("CassandraSchemaOperationTest",
                propertyMap);
    }
}
