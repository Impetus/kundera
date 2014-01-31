/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.oraclenosql;

import java.io.File;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.oraclenosql.entities.UserProfile;

/**
 * Test case for read and write on Large Objects (LOBs)
 * 
 * @author amresh.singh
 */
public class OracleNoSQLLOBTest extends OracleNoSQLTestBase
{
    @Before
    public void setUp() throws Exception
    {
        super.setUp();

        setEmProperty("write.timeout", 10);
        setEmProperty("durability", Durability.COMMIT_WRITE_NO_SYNC);
        setEmProperty("time.unit", TimeUnit.SECONDS);
        setEmProperty("consistency", Consistency.NONE_REQUIRED);
    }

    @After
    public void tearDown()
    {
        super.tearDown();
        
    }

    @Test
    public void executeLOBTest()
    {
        // Save Record
        File file = new File("src/test/resources/nature.jpg");
        long fileSize = file.getTotalSpace();
        UserProfile userProfile = new UserProfile(1, "Amresh", file);
        persist(userProfile);

        // Find Record
        clearEm();
        UserProfile up = (UserProfile) find(UserProfile.class, 1);
        Assert.assertNotNull(up);
        Assert.assertEquals(1, up.getUserId());
        Assert.assertEquals("Amresh", up.getUserName());
        Assert.assertEquals(fileSize, up.getProfilePicture().getTotalSpace());

        // Delete Record
//        clearEm();
        delete(up);
        Assert.assertNull(find(UserProfile.class, 1));

    }
}
