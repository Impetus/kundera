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
package com.impetus.kundera.tests.crossdatastore.pickr;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.tests.cli.CassandraCli;
import com.impetus.kundera.tests.cli.HBaseCli;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.album.AlbumBi_M_M_1_1;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.photo.PhotoBi_M_M_1_1;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.photographer.PhotographerBi_M_M_1_1;

/**
 * @author amresh.singh
 * 
 */
public class PickrTestBi_M_M_1_1 extends PickrBaseTest
{

    private static Log log = LogFactory.getLog(PickrTestBi_M_M_1_1.class);

    @Before
    public void setUp() throws Exception
    {
        super.setUp();
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
        super.tearDown();
    }

    @Test
    public void test()
    {
        executeTests();
    }

    @Override
    public void addPhotographer()
    {
        List<PhotographerBi_M_M_1_1> ps = populatePhotographers();

        for (PhotographerBi_M_M_1_1 p : ps)
        {
            pickr.addPhotographer(p);
        }
    }

    @Override
    protected void getPhotographer()
    {
        PhotographerBi_M_M_1_1 p1 = (PhotographerBi_M_M_1_1) pickr.getPhotographer(PhotographerBi_M_M_1_1.class, 1);
        assertPhotographer(p1, 1);

        PhotographerBi_M_M_1_1 p2 = (PhotographerBi_M_M_1_1) pickr.getPhotographer(PhotographerBi_M_M_1_1.class, 2);
        assertPhotographer(p2, 2);
    }

    @Override
    protected void updatePhotographer()
    {
        PhotographerBi_M_M_1_1 p1 = (PhotographerBi_M_M_1_1) pickr.getPhotographer(PhotographerBi_M_M_1_1.class, 1);
        assertPhotographer(p1, 1);
        p1.setPhotographerName("Amresh2");

        pickr.mergePhotographer(p1);

        PhotographerBi_M_M_1_1 p1Modified = (PhotographerBi_M_M_1_1) pickr.getPhotographer(
                PhotographerBi_M_M_1_1.class, 1);
        assertModifiedPhotographer(p1Modified, 1);

        PhotographerBi_M_M_1_1 p2 = (PhotographerBi_M_M_1_1) pickr.getPhotographer(PhotographerBi_M_M_1_1.class, 2);
        assertPhotographer(p2, 2);
        p2.setPhotographerName("Vivek2");

        pickr.mergePhotographer(p2);

        PhotographerBi_M_M_1_1 p2Modified = (PhotographerBi_M_M_1_1) pickr.getPhotographer(
                PhotographerBi_M_M_1_1.class, 2);
        assertModifiedPhotographer(p2Modified, 2);
    }

    @Override
    protected void getAllPhotographers()
    {
        List<Object> ps = pickr.getAllPhotographers(PhotographerBi_M_M_1_1.class.getSimpleName());

        for (Object p : ps)
        {
            PhotographerBi_M_M_1_1 pp = (PhotographerBi_M_M_1_1) p;
            Assert.assertNotNull(pp);
            assertModifiedPhotographer(pp, pp.getPhotographerId());
        }

    }

    @Override
    protected void deletePhotographer()
    {
        PhotographerBi_M_M_1_1 p1 = (PhotographerBi_M_M_1_1) pickr.getPhotographer(PhotographerBi_M_M_1_1.class, 1);
        assertModifiedPhotographer(p1, 1);
        pickr.deletePhotographer(p1);

        PhotographerBi_M_M_1_1 p1AfterDeletion = (PhotographerBi_M_M_1_1) pickr.getPhotographer(
                PhotographerBi_M_M_1_1.class, 1);
        Assert.assertNull(p1AfterDeletion);
    }

    private void assertPhotographer(PhotographerBi_M_M_1_1 p, int photographerId)
    {

        if (photographerId == 1)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(1, p.getPhotographerId());
            Assert.assertEquals("Amresh", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbums());
            Assert.assertFalse(p.getAlbums().isEmpty());
            Assert.assertEquals(2, p.getAlbums().size());

            AlbumBi_M_M_1_1 album1 = p.getAlbums().get(0);
            Assert.assertNotNull(album1);
            Assert.assertTrue(album1.getAlbumId().startsWith("album_"));
            PhotoBi_M_M_1_1 photo1 = album1.getPhoto();
            Assert.assertNotNull(photo1);
            Assert.assertTrue(photo1.getPhotoId().startsWith("photo_"));

            AlbumBi_M_M_1_1 album2 = p.getAlbums().get(1);
            Assert.assertNotNull(album2);
            Assert.assertTrue(album2.getAlbumId().startsWith("album_"));
            PhotoBi_M_M_1_1 photo2 = album2.getPhoto();
            Assert.assertNotNull(photo2);
            Assert.assertTrue(photo2.getPhotoId().startsWith("photo_"));

        }
        else if (photographerId == 2)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(2, p.getPhotographerId());
            Assert.assertEquals("Vivek", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbums());
            Assert.assertFalse(p.getAlbums().isEmpty());
            Assert.assertEquals(2, p.getAlbums().size());

            AlbumBi_M_M_1_1 album1 = p.getAlbums().get(0);
            Assert.assertNotNull(album1);
            Assert.assertTrue(album1.getAlbumId().startsWith("album_"));
            PhotoBi_M_M_1_1 photo1 = album1.getPhoto();
            Assert.assertNotNull(photo1);
            Assert.assertTrue(photo1.getPhotoId().startsWith("photo_"));

            AlbumBi_M_M_1_1 album2 = p.getAlbums().get(1);
            Assert.assertNotNull(album2);
            Assert.assertTrue(album2.getAlbumId().startsWith("album_"));
            PhotoBi_M_M_1_1 photo2 = album2.getPhoto();
            Assert.assertNotNull(photo2);
            Assert.assertTrue(photo2.getPhotoId().startsWith("photo_"));
        }
        else
        {
            Assert.fail("Invalid Photographer ID: " + photographerId);
        }

    }

    private void assertModifiedPhotographer(PhotographerBi_M_M_1_1 p, int photographerId)
    {

        if (photographerId == 1)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(1, p.getPhotographerId());
            Assert.assertEquals("Amresh2", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbums());
            Assert.assertFalse(p.getAlbums().isEmpty());
            Assert.assertEquals(2, p.getAlbums().size());

            AlbumBi_M_M_1_1 album1 = p.getAlbums().get(0);
            Assert.assertNotNull(album1);
            Assert.assertTrue(album1.getAlbumId().startsWith("album_"));
            PhotoBi_M_M_1_1 photo1 = album1.getPhoto();
            Assert.assertNotNull(photo1);
            Assert.assertTrue(photo1.getPhotoId().startsWith("photo_"));

            AlbumBi_M_M_1_1 album2 = p.getAlbums().get(1);
            Assert.assertNotNull(album2);
            Assert.assertTrue(album2.getAlbumId().startsWith("album_"));
            PhotoBi_M_M_1_1 photo2 = album2.getPhoto();
            Assert.assertNotNull(photo2);
            Assert.assertTrue(photo2.getPhotoId().startsWith("photo_"));

        }
        else if (photographerId == 2)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(2, p.getPhotographerId());
            Assert.assertEquals("Vivek2", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbums());
            Assert.assertFalse(p.getAlbums().isEmpty());
            Assert.assertEquals(2, p.getAlbums().size());

            AlbumBi_M_M_1_1 album1 = p.getAlbums().get(0);
            Assert.assertNotNull(album1);
            Assert.assertTrue(album1.getAlbumId().startsWith("album_"));
            PhotoBi_M_M_1_1 photo1 = album1.getPhoto();
            Assert.assertNotNull(photo1);
            Assert.assertTrue(photo1.getPhotoId().startsWith("photo_"));

            AlbumBi_M_M_1_1 album2 = p.getAlbums().get(1);
            Assert.assertNotNull(album2);
            Assert.assertTrue(album2.getAlbumId().startsWith("album_"));
            PhotoBi_M_M_1_1 photo2 = album2.getPhoto();
            Assert.assertNotNull(photo2);
            Assert.assertTrue(photo2.getPhotoId().startsWith("photo_"));
        }
        else
        {
            Assert.fail("Invalid Photographer ID: " + photographerId);
        }

    }

    private List<PhotographerBi_M_M_1_1> populatePhotographers()
    {
        List<PhotographerBi_M_M_1_1> photographers = new ArrayList<PhotographerBi_M_M_1_1>();

        // Photographer 1
        PhotographerBi_M_M_1_1 p1 = new PhotographerBi_M_M_1_1();
        p1.setPhotographerId(1);
        p1.setPhotographerName("Amresh");

        // Photographer 2
        PhotographerBi_M_M_1_1 p2 = new PhotographerBi_M_M_1_1();
        p2.setPhotographerId(2);
        p2.setPhotographerName("Vivek");

        AlbumBi_M_M_1_1 album1 = new AlbumBi_M_M_1_1("album_1", "My Phuket Vacation", "Went Phuket with friends");
        AlbumBi_M_M_1_1 album2 = new AlbumBi_M_M_1_1("album_2", "My Shimla Vacation", "Went Shimla with friends");
        AlbumBi_M_M_1_1 album3 = new AlbumBi_M_M_1_1("album_3", "My Zurik Vacation", "Went Zurik with friends");

        album1.setPhoto(new PhotoBi_M_M_1_1("photo_1", "One beach", "On beach with friends"));
        album2.setPhoto(new PhotoBi_M_M_1_1("photo_2", "In Hotel", "Chilling out in room"));
        album3.setPhoto(new PhotoBi_M_M_1_1("photo_3", "At Airport", "So tired"));

        p1.addAlbum(album1);
        p1.addAlbum(album2);

        p2.addAlbum(album2);
        p2.addAlbum(album3);

        photographers.add(p1);
        photographers.add(p2);

        return photographers;
    }

    @Override
    protected void startServer() throws IOException, TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {

        if (RUN_IN_EMBEDDED_MODE)
        {
            CassandraCli.cassandraSetUp();
            // HBaseCli.startCluster();
        }
        if (AUTO_MANAGE_SCHEMA)
        {

            /**
             * schema generation for cassandra.
             * */

            KsDef ksDef = null;

            CfDef pCfDef = new CfDef();
            pCfDef.name = "PHOTOGRAPHER";
            pCfDef.keyspace = "Pickr";
            pCfDef.setComparator_type("UTF8Type");
            pCfDef.setDefault_validation_class("UTF8Type");
            ColumnDef pColumnDef2 = new ColumnDef(ByteBuffer.wrap("PHOTOGRAPHER_NAME".getBytes()), "UTF8Type");
            pColumnDef2.index_type = IndexType.KEYS;
            pCfDef.addToColumn_metadata(pColumnDef2);

            CfDef aCfDef = new CfDef();
            aCfDef.name = "ALBUM";
            aCfDef.keyspace = "Pickr";
            aCfDef.setComparator_type("UTF8Type");
            aCfDef.setDefault_validation_class("UTF8Type");
            ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("ALBUM_NAME".getBytes()), "UTF8Type");
            columnDef.index_type = IndexType.KEYS;
            ColumnDef columnDef3 = new ColumnDef(ByteBuffer.wrap("ALBUM_DESC".getBytes()), "UTF8Type");
            columnDef3.index_type = IndexType.KEYS;
            ColumnDef columnDef5 = new ColumnDef(ByteBuffer.wrap("PHOTO_ID".getBytes()), "UTF8Type");
            columnDef5.index_type = IndexType.KEYS;
            aCfDef.addToColumn_metadata(columnDef);
            aCfDef.addToColumn_metadata(columnDef5);
            aCfDef.addToColumn_metadata(columnDef3);

            CfDef photoLinkCfDef = new CfDef();
            photoLinkCfDef.name = "PHOTO";
            photoLinkCfDef.keyspace = "Pickr";
            photoLinkCfDef.setComparator_type("UTF8Type");
            photoLinkCfDef.setDefault_validation_class("UTF8Type");
            ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("PHOTO_CAPTION".getBytes()), "UTF8Type");
            columnDef1.index_type = IndexType.KEYS;
            ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("PHOTO_DESC".getBytes()), "UTF8Type");
            columnDef2.index_type = IndexType.KEYS;
            photoLinkCfDef.addToColumn_metadata(columnDef1);
            photoLinkCfDef.addToColumn_metadata(columnDef2);

            CfDef cfDef = new CfDef();
            cfDef.name = "PHOTOGRAPHER_ALBUM";
            cfDef.keyspace = "Pickr";
            cfDef.setComparator_type("UTF8Type");
            cfDef.setDefault_validation_class("UTF8Type");
            ColumnDef columnDef4 = new ColumnDef(ByteBuffer.wrap("PHOTOGRAPHER_ID".getBytes()), "UTF8Type");
            columnDef1.index_type = IndexType.KEYS;
            ColumnDef columnDef6 = new ColumnDef(ByteBuffer.wrap("ALBUM_ID".getBytes()), "UTF8Type");
            columnDef2.index_type = IndexType.KEYS;
            cfDef.addToColumn_metadata(columnDef4);
            cfDef.addToColumn_metadata(columnDef6);

            List<CfDef> cfDefs = new ArrayList<CfDef>();
            cfDefs.add(pCfDef);
            cfDefs.add(aCfDef);
            cfDefs.add(cfDef);
            cfDefs.add(photoLinkCfDef);
            try
            {
                ksDef = CassandraCli.client.describe_keyspace("Pickr");
                CassandraCli.client.set_keyspace("Pickr");
                List<CfDef> cfDefn = ksDef.getCf_defs();

                for (CfDef cfDef1 : cfDefn)
                {

                    if (cfDef1.getName().equalsIgnoreCase("PHOTOGRAPHER"))
                    {
                        CassandraCli.client.system_drop_column_family("PHOTOGRAPHER");
                    }
                    if (cfDef1.getName().equalsIgnoreCase("ALBUM"))
                    {
                        CassandraCli.client.system_drop_column_family("ALBUM");
                    }
                    if (cfDef1.getName().equalsIgnoreCase("PHOTO"))
                    {
                        CassandraCli.client.system_drop_column_family("PHOTO");
                    }
                    if (cfDef1.getName().equalsIgnoreCase("PHOTOGRAPHER_ALBUM"))
                    {
                        CassandraCli.client.system_drop_column_family("PHOTOGRAPHER_ALBUM");
                    }
                }
                CassandraCli.client.system_add_column_family(pCfDef);
                CassandraCli.client.system_add_column_family(aCfDef);
                CassandraCli.client.system_add_column_family(photoLinkCfDef);
                CassandraCli.client.system_add_column_family(cfDef);
            }
            catch (NotFoundException e)
            {
                addKeyspace(ksDef, cfDefs);
            }
            catch (InvalidRequestException e)
            {
                log.error(e.getMessage());
            }
            catch (TException e)
            {
                log.error(e.getMessage());
            }

        }

    }

    @Override
    protected void stopServer()
    {
        HBaseCli.stopCluster();

    }

}
