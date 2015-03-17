/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.crud.datatypes;

import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.commons.codec.binary.Hex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.client.crud.BaseTest;
import com.impetus.client.crud.PersonCassandra;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * Test case to perform simple CRUD operation.(insert, delete, merge, and
 * select)
 * 
 * @author chhavi.gangwal
 * 
 *         Run this script to create column family in cassandra with indexes.
 *         create column family PERSON with comparator=UTF8Type and
 *         column_metadata=[{column_name: PERSON_NAME, validation_class:
 *         UTF8Type, index_type: KEYS}, {column_name: AGE, validation_class:
 *         IntegerType, index_type: KEYS}];
 * 
 */
public class ByteDataTest extends BaseTest
{
    private static final String SEC_IDX_CASSANDRA_TEST = "genericCassandraTest";

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /** The col. */
    private Map<Object, Object> col;

    protected Map propertyMap = null;

    protected boolean AUTO_MANAGE_SCHEMA = true;

    protected boolean USE_CQL = false;

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

        if (propertyMap == null)
        {
            propertyMap = new HashMap();
            propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        }
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);

        emf = Persistence.createEntityManagerFactory(SEC_IDX_CASSANDRA_TEST, propertyMap);
        em = emf.createEntityManager();
        col = new java.util.HashMap<Object, Object>();
    }

    /**
     * On insert image in cassandra blob object
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onInsertBlobImageCassandra() throws Exception
    {

        PersonCassandra personWithKey = new PersonCassandra();
        personWithKey.setPersonId("111");

        BufferedImage originalImage = ImageIO.read(new File("src/test/resources/nature.jpg"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(originalImage, "jpg", baos);
        baos.flush();
        byte[] imageInByte = baos.toByteArray();

        baos.close();

        personWithKey.setA(imageInByte);
        em.persist(personWithKey);

        em.clear();

        String qry = "Select p from PersonCassandra p where p.personId = 111";
        Query q = em.createQuery(qry);
        List<PersonCassandra> persons = q.getResultList();
        PersonCassandra person = persons.get(0);

        InputStream in = new ByteArrayInputStream(person.getA());
        BufferedImage bImageFromConvert = ImageIO.read(in);

        ImageIO.write(bImageFromConvert, "jpg", new File("src/test/resources/nature-test.jpg"));

        Assert.assertNotNull(person.getA());
        Assert.assertEquals(new File("src/test/resources/nature.jpg").getTotalSpace(), new File(
                "src/test/resources/nature-test.jpg").getTotalSpace());
        Assert.assertEquals(String.valueOf(Hex.encodeHex((byte[]) imageInByte)),
                String.valueOf(Hex.encodeHex((byte[]) person.getA())));

    }

    /**
     * On insert pdf in cassandra blob object
     * 
     * @throws Exception
     *             the exception
     */
    @Test
    public void onInsertBlobPdfCassandra() throws Exception
    {

        try
        {

            FileInputStream fileInputStream = null;

            File file = new File("src/test/resources/persistence.pdf");

            byte[] bFile = new byte[(int) file.length()];

            PersonCassandra personWithKey = new PersonCassandra();
            personWithKey.setPersonId("111");
            // convert file into array of bytes
            fileInputStream = new FileInputStream(file);
            fileInputStream.read(bFile);
            fileInputStream.close();

            personWithKey.setA(bFile);
            em.persist(personWithKey);
            em.clear();

            String qry = "Select p from PersonCassandra p where p.personId = 111";
            Query q = em.createQuery(qry);
            List<PersonCassandra> persons = q.getResultList();
            PersonCassandra person = persons.get(0);

            // convert array of bytes into file
            FileOutputStream fileOuputStream = new FileOutputStream("src/test/resources/persistence-test.pdf");
            fileOuputStream.write(person.getA());
            fileOuputStream.close();

            Assert.assertNotNull(person.getA());
            Assert.assertEquals(new File("src/test/resources/persistence.pdf").getTotalSpace(), new File(
                    "src/test/resources/persistence-test.pdf").getTotalSpace());
            Assert.assertTrue(isFileBinaryEqual(new File("src/test/resources/persistence.pdf"), new File(
                    "src/test/resources/persistence-test.pdf")));
            Assert.assertEquals(String.valueOf(Hex.encodeHex((byte[]) bFile)),
                    String.valueOf(Hex.encodeHex((byte[]) person.getA())));

        }
        catch (Exception e)
        {

        }

    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {/*
      * Delete is working, but as row keys are not deleted from cassandra, so
      * resulting in issue while reading back. // Delete
      * em.remove(em.find(Person.class, "1")); em.remove(em.find(Person.class,
      * "2")); em.remove(em.find(Person.class, "3")); em.close(); emf.close();
      * em = null; emf = null;
      */
        em.close();
        emf.close();
        CassandraCli.dropKeySpace("KunderaExamples");
    }

    /**
     * Compare binary files. Both files must be files (not directories) and
     * exist.
     * 
     * @param first
     *            - first file
     * @param second
     *            - second file
     * @return boolean - true if files are binary equal
     * @throws IOException
     *             - error in function
     */
    private boolean isFileBinaryEqual(File first, File second) throws IOException
    {

        boolean retval = false;

        if ((first.exists()) && (second.exists()) && (first.isFile()) && (second.isFile()))
        {
            if (first.getCanonicalPath().equals(second.getCanonicalPath()))
            {
                retval = true;
            }
            else
            {
                FileInputStream firstInput = new FileInputStream(first);
                FileInputStream secondInput = new FileInputStream(second);
                BufferedInputStream bufFirstInput = new BufferedInputStream(firstInput);
                BufferedInputStream bufSecondInput = new BufferedInputStream(secondInput);

                try
                {

                    int firstByte;
                    int secondByte;

                    while (true)
                    {
                        firstByte = bufFirstInput.read();
                        secondByte = bufSecondInput.read();
                        if (firstByte != secondByte)
                        {
                            break;
                        }
                        if ((firstByte < 0) && (secondByte < 0))
                        {
                            retval = true;
                            break;
                        }
                    }
                }
                finally
                {
                    try
                    {
                        if (bufFirstInput != null)
                        {
                            bufFirstInput.close();
                        }
                    }
                    finally
                    {
                        if (bufSecondInput != null)
                        {
                            bufSecondInput.close();
                        }
                    }
                }
            }
        }

        return retval;
    }

}
