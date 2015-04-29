/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.crud.gfs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The Class GridFSTest.
 * 
 * @author Devender Yadav
 */
public class GridFSTest
{

    /** The Constant _PU. */
    private static final String _PU = "gfs_pu";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /**
     * Sets the up before class.
     * 
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void SetUpBeforeClass() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(_PU);
    }

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        em = emf.createEntityManager();
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
        em.close();
    }

    /**
     * Tear down after class.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        emf.close();
    }

    /**
     * Test crud grid fs.
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void testCRUDGridFS() throws Exception
    {
        testInsert();
        testUpdateNonLobField();
        testUpdateLobField();
        testDelete();
    }

    /**
     * Test insert.
     */
    private void testInsert()
    {
        byte[] profilePic = createBinaryData("src/test/resources/pic.jpg", 20);
        GFSUser user = prepareUserObject("1", "Dev", profilePic);
        em.persist(user);

        em.clear();

        GFSUser u = em.find(GFSUser.class, "1");
        Assert.assertNotNull(u);
        Assert.assertEquals("1", u.getUserId());
        Assert.assertEquals("Dev", u.getName());
        Assert.assertEquals(profilePic.length, u.getProfilePic().length);
    }

    /**
     * Test update non lob field.
     */
    private void testUpdateNonLobField()
    {
        GFSUser user = em.find(GFSUser.class, "1");
        user.setName("Devender");
        em.merge(user);

        em.clear();

        GFSUser u1 = em.find(GFSUser.class, "1");

        Assert.assertNotNull(u1);
        Assert.assertEquals("1", u1.getUserId());
        Assert.assertEquals("Devender", u1.getName());
    }

    /**
     * Test update lob field.
     */
    private void testUpdateLobField()
    {
        GFSUser user = em.find(GFSUser.class, "1");
        byte[] profilePic = createBinaryData("src/test/resources/pic.jpg", 15);
        user.setProfilePic(profilePic);

        em.merge(user);

        em.clear();

        GFSUser u = em.find(GFSUser.class, "1");

        Assert.assertNotNull(u);
        Assert.assertEquals("1", u.getUserId());
        Assert.assertEquals("Devender", u.getName());
        Assert.assertEquals(profilePic.length, u.getProfilePic().length);
    }

    /**
     * Test delete.
     */
    private void testDelete()
    {
        GFSUser user = em.find(GFSUser.class, "1");
        em.remove(user);
        em.clear();
        GFSUser u = em.find(GFSUser.class, "1");
        Assert.assertNull(u);
    }

    /**
     * Prepare user object.
     * 
     * @param userID
     *            the user id
     * @param name
     *            the name
     * @param profilePic
     *            the profile pic
     * @return the GFS user
     */
    private GFSUser prepareUserObject(String userID, String name, byte[] profilePic)
    {
        GFSUser user = new GFSUser();
        user.setUserId(userID);
        user.setName(name);
        user.setProfilePic(profilePic);
        return user;
    }

    /**
     * Creates the binary data.
     * 
     * @param locationPath
     *            the location path
     * @param multiplicationFactor
     *            the multiplication factor
     * @return the byte[]
     */
    private byte[] createBinaryData(String locationPath, int multiplicationFactor)
    {
        Path path = Paths.get(locationPath);
        byte[] data = null;
        try
        {
            data = Files.readAllBytes(path);
        }
        catch (IOException e)
        {
            Assert.fail();
        }

        byte[] multipliedData = new byte[data.length * multiplicationFactor];

        for (int i = 0; i < multiplicationFactor; i++)
        {
            System.arraycopy(data, 0, multipliedData, i * data.length, data.length);
        }

        return multipliedData;
    }

}
