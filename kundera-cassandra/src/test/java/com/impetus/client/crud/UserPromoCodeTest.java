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
package com.impetus.client.crud;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.entity.PromoCode;
import com.impetus.client.entity.Users;
import com.impetus.client.persistence.CassandraCli;


public class UserPromoCodeTest
{
    private static final String SEC_IDX_CASSANDRA_TEST = "secIdxCassandraTest";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("KunderaExamples");
        loadData();
        emf = Persistence.createEntityManagerFactory(SEC_IDX_CASSANDRA_TEST);
        em = emf.createEntityManager();
    }

    /**
     * On insert cassandra.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onInsertUsers() throws Exception
    {
        Users u = new Users();
        u.setFirstName("firstname");
        u.setLastName("lastname");
        u.setUserId("1_u");

        PromoCode promoCode1 = new PromoCode();
        promoCode1.setPromoCodeId("1_p");
        promoCode1.setPromoCodeName("promoname1");

        u.getPromoCodes().add(promoCode1);
        
        em.persist(u);
        
        em.clear(); // to avoid fetch from cache and get from DB.
        
        Users found = em.find(Users.class, "1_u");
        
        Assert.assertNotNull(found);
        Assert.assertEquals("firstname", found.getFirstName());
        Assert.assertEquals(1, found.getPromoCodes().size());
        
        // add 1 more promo code.
        
       PromoCode promoCode = new PromoCode();
       promoCode.setPromoCodeId("2_p");
       promoCode.setPromoCodeName("promoname2");
       
       found.getPromoCodes().add(promoCode);
       em.merge(found);
       
       
       em.clear(); // to avoid fetch from cache get from DB.
       
       found = em.find(Users.class, "1_u");
       
       Assert.assertNotNull(found);
       Assert.assertEquals("firstname", found.getFirstName());
       Assert.assertEquals(2, found.getPromoCodes().size());
       
       
       // Delete
       em.remove(found);

       
       found = em.find(Users.class, "1_u");
       
       Assert.assertNull(found);

    }

    /**
     * Load cassandra specific data.
     * 
     * @throws TException
     *             the t exception
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws UnavailableException
     *             the unavailable exception
     * @throws TimedOutException
     *             the timed out exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     */
    private void loadData() throws TException, InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException
    {
        KsDef ksDef = null;
        CfDef user_Def = new CfDef();
        user_Def.name = "users";
        user_Def.keyspace = "KunderaExamples";
        user_Def.setComparator_type("UTF8Type");
        user_Def.setDefault_validation_class("UTF8Type");
        user_Def.setColumn_type("Super");

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace("KunderaExamples");
            CassandraCli.client.set_keyspace("KunderaExamples");

            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase("users"))
                {

                    CassandraCli.client.system_drop_column_family("users");

                }
            }
            CassandraCli.client.system_add_column_family(user_Def);

        }
        catch (NotFoundException e)
        {

            ksDef = new KsDef("KunderaExamples", "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
            // Set replication factor
            if (ksDef.strategy_options == null)
            {
                ksDef.strategy_options = new LinkedHashMap<String, String>();
            }
            // Set replication factor, the value MUST be an integer
            ksDef.strategy_options.put("replication_factor", "1");
            CassandraCli.client.system_add_keyspace(ksDef);
        }

        CassandraCli.client.set_keyspace("KunderaExamples");

    }

}
