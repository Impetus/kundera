/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.tests.file.dao;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;
import com.impetus.kundera.tests.file.entities.ProfilePicture;

/**
 * Test case for {@link ProfilePictureDao}
 * 
 * @author amresh.singh
 */
public class ProfilePictureDaoTest
{
    String inputFilePath;

    ProfilePictureDao dao;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        inputFilePath = "/home/impadmin/input.jpg";
        dao = new ProfilePictureDao("secIdxAddCassandra");
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        dao.closeEntityManagerfactory();
    }

    @Test
    public void dummyTest()
    {

    }

    /**
     * Test method for
     * {@link com.impetus.kundera.tests.file.dao.ProfilePictureDao#addProfilePicture(int, java.io.File)}
     * .
     */
    // @Test
    public void test()
    {
        // Insert picture
        int id = 1;
        File fullPictureFile = new File(inputFilePath);
        dao.addProfilePicture(id, fullPictureFile);

        // Find Picture
        ProfilePicture pp = dao.getProfilePicture(1);
        try
        {
            Files.write(pp.getFullPicture(), new File("/home/impadmin/output.jpg"));
        }
        catch (IOException e)
        {
            Assert.fail(e.getMessage());
        }
    }

}
