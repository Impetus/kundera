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

import javax.persistence.EntityManager;

import junit.framework.TestCase;

import com.impetus.hbase.entity.HUser;
import com.impetus.kundera.entity.PersonalDetail;
import com.impetus.kundera.entity.Tweet;
import com.impetus.kundera.loader.Configuration;

/**
 * @author impetus
 * 
 */
public class TestHBase extends TestCase
{

    Configuration conf;

    /** The manager. */
    private EntityManager manager;

    /**
     * Sets the up.
     * 
     * @throws java.lang.Exception
     *             * @throws Exception the exception
     * @throws Exception
     *             the exception
     */
    @Override
    public void setUp() throws Exception
    {
        conf = new Configuration();
        manager = conf.getEntityManager("hbase");

    }

    public void testSaveUser()
    {
        HUser user = new HUser();
        user.setUserId("0001");

        PersonalDetail pd = new PersonalDetail();
        pd.setPersonalDetailId("a");
        pd.setName("Amresh");
        pd.setPassword("password1");
        pd.setRelationshipStatus("Single");

        user.setPersonalDetail(pd);

        user.addTweet(new Tweet("1", "My first tweet", "Web"));
        user.addTweet(new Tweet("2", "My second tweet", "Mobile"));

        manager.persist(user);
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        conf.destroy();
    }

}
