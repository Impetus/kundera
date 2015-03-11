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
package com.impetus.client.twitter;

import org.junit.Test;

import com.impetus.client.hbase.junits.HBaseCli;

/**
 * Test case for Twitter like application on HBase
 * 
 * @author amresh.singh
 */
public class TwibaseTest extends TwitterTestBaseHbase
{

    HBaseCli cli = new HBaseCli();

    @Override
    protected void setUp() throws Exception
    {
        
        setUpInternal("twibaseTest");
    }

    /**
     * Test on execute.
     */
    @Test
    public void testOnExecute()
    {
        executeTestSuite();
    }

    @Override
    protected void tearDown() throws Exception
    {
        tearDownInternal();
    }

    @Override
    void startServer()
    {
        cli.startCluster();
    }

    @Override
    void stopServer()
    {
        cli.stopCluster();
    }

    @Override
    void createSchema()
    {
        if (AUTO_MANAGE_SCHEMA)
        {/*
          * cli.createTable("USER"); cli.addColumnFamily("USER",
          * "PREFERENCE_ID"); cli.addColumnFamily("USER", "FRIEND_ID");
          * cli.addColumnFamily("USER", "FOLLOWER_ID");
          * cli.addColumnFamily("USER", "personalDetail");
          * 
          * cli.createTable("PREFERENCE"); cli.addColumnFamily("PREFERENCE",
          * "WEBSITE_THEME"); cli.addColumnFamily("PREFERENCE",
          * "PRIVACY_LEVEL");
          * 
          * cli.createTable("EXTERNAL_LINK");
          * cli.addColumnFamily("EXTERNAL_LINK", "LINK_TYPE");
          * cli.addColumnFamily("EXTERNAL_LINK", "USER_ID");
          * cli.addColumnFamily("EXTERNAL_LINK", "LINK_ADDRESS");
          */
        }
    }

    @Override
    void deleteSchema()
    {
        cli.dropTable("KunderaExamples");
    }
}
