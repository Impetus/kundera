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
package com.impetus.kundera.junit;

import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import junit.framework.TestCase;

import com.impetus.hbase.entity.HAuthor;
import com.impetus.hbase.entity.HUser;
import com.impetus.kundera.entity.PersonalDetail;
import com.impetus.kundera.loader.Configuration;

/**
 * @author impetus
 *
 */
public class TestHBase extends TestCase
{

    /** The manager. */
    private EntityManager manager;

    
     /**
     * Sets the up.
     *
     * @throws java.lang.Exception * @throws Exception the exception
     * @throws Exception the exception
     */
     public void setUp() throws Exception {
     Configuration conf = new Configuration();
     manager = conf.getEntityManager("hbase");
    
     }

    /*@SuppressWarnings("unchecked")
    public void testOnPersist()
    {
        HAuthor author = createAuthor("vivek", "vivek@vivek.com", "India", new Date());
        manager.persist(author);
        HAuthor animesh_db = manager.find(HAuthor.class, "vivek");
        assertEquals(animesh, animesh_db);
        Query query = manager.createQuery("select a from HAuthor a where a.country like :country");
        query.setParameter("country", "India");
        List<HAuthor> list = query.getResultList();
        assertNotNull(list);
        for (HAuthor auth : list)
        {
            System.out.println("called");
            assertEquals("vivek", auth.getUsername());
            assertEquals("vivek@vivek.com", auth.getEmailAddress());
        }
        System.out.println(author);
     
    }*/
    
    public void testSaveUser() {
        HUser user = new HUser();
        user.setUserId("0001");
        
        PersonalDetail pd = new PersonalDetail();
        pd.setPersonalDetailId("a");
        pd.setName("Amresh");
        pd.setPassword("password1");
        pd.setRelationshipStatus("Single");
        
        user.setPersonalDetail(pd);
        manager.persist(user);
    }  
    

    /**
     * Creates the author.
     *
     * @param username
     *            the user name
     * @param email
     *            the email
     * @param country
     *            the country
     * @param registeredSince
     *            the registered since
     *
     * @return the author
     */
    /*private static HAuthor createAuthor(String username, String email, String country, Date registeredSince)
    {
        HAuthor author = new HAuthor();
        author.setUsername(username);
        author.setCountry(country);
        author.setEmailAddress(email);
        author.setRegistered(registeredSince);
        return author;
    }*/

}
