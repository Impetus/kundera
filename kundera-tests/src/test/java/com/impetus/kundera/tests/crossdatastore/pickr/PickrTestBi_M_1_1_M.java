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
import com.impetus.kundera.tests.crossdatastore.pickr.entities.album.AlbumBi_M_1_1_M;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.photo.PhotoBi_M_1_1_M;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.photographer.PhotographerBi_M_1_1_M;

/**
 * @author amresh.singh
 * 
 */
public class PickrTestBi_M_1_1_M extends PickrBaseTest
{
    private static Log log = LogFactory.getLog(PickrTestBi_M_1_1_M.class);

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

    /**
     * Test.
     */
    @Test
    public void test()
    {
        executeTests();

    }

    @Override
    public void addPhotographer()
    {
        List<PhotographerBi_M_1_1_M> ps = populatePhotographers();

        for (PhotographerBi_M_1_1_M p : ps)
        {
            pickr.addPhotographer(p);
        }
    }

    @Override
    protected void getPhotographer()
    {
        PhotographerBi_M_1_1_M p1 = (PhotographerBi_M_1_1_M) pickr
                .getPhotographer(PhotographerBi_M_1_1_M.class, "" + 1);
        assertPhotographer(p1, 1);

        PhotographerBi_M_1_1_M p2 = (PhotographerBi_M_1_1_M) pickr
                .getPhotographer(PhotographerBi_M_1_1_M.class, "" + 2);
        assertPhotographer(p2, 2);
    }

    @Override
    protected void updatePhotographer()
    {
        PhotographerBi_M_1_1_M p1 = (PhotographerBi_M_1_1_M) pickr
                .getPhotographer(PhotographerBi_M_1_1_M.class, "" + 1);
        assertPhotographer(p1, 1);
        p1.setPhotographerName("Amresh2");

        pickr.mergePhotographer(p1);

        PhotographerBi_M_1_1_M p1Modified = (PhotographerBi_M_1_1_M) pickr.getPhotographer(
                PhotographerBi_M_1_1_M.class, "" + 1);
        assertModifiedPhotographer(p1Modified, 1);

        PhotographerBi_M_1_1_M p2 = (PhotographerBi_M_1_1_M) pickr
                .getPhotographer(PhotographerBi_M_1_1_M.class, "" + 2);
        assertPhotographer(p2, 2);
        p2.setPhotographerName("Vivek2");

        pickr.mergePhotographer(p2);

        PhotographerBi_M_1_1_M p2Modified = (PhotographerBi_M_1_1_M) pickr.getPhotographer(
                PhotographerBi_M_1_1_M.class, "" + 2);
        assertModifiedPhotographer(p2Modified, 2);
    }

    @Override
    protected void getAllPhotographers()
    {
        List<Object> ps = pickr.getAllPhotographers(PhotographerBi_M_1_1_M.class.getSimpleName());

        for (Object p : ps)
        {
            PhotographerBi_M_1_1_M pp = (PhotographerBi_M_1_1_M) p;
            Assert.assertNotNull(pp);
            assertModifiedPhotographer(pp, pp.getPhotographerId());
        }

    }

    @Override
    protected void deletePhotographer()
    {
        PhotographerBi_M_1_1_M p1 = (PhotographerBi_M_1_1_M) pickr
                .getPhotographer(PhotographerBi_M_1_1_M.class, "" + 1);
        assertModifiedPhotographer(p1, 1);
        pickr.deletePhotographer(p1);

        PhotographerBi_M_1_1_M p1AfterDeletion = (PhotographerBi_M_1_1_M) pickr.getPhotographer(
                PhotographerBi_M_1_1_M.class, "" + 1);
        Assert.assertNull(p1AfterDeletion);

        PhotographerBi_M_1_1_M p2 = (PhotographerBi_M_1_1_M) pickr
                .getPhotographer(PhotographerBi_M_1_1_M.class, "" + 2);
        Assert.assertNotNull(p2);
        pickr.deletePhotographer(p2);

        PhotographerBi_M_1_1_M p2AfterDeletion = (PhotographerBi_M_1_1_M) pickr.getPhotographer(
                PhotographerBi_M_1_1_M.class, "" + 2);
        Assert.assertNull(p2AfterDeletion);

    }

    private void assertPhotographer(PhotographerBi_M_1_1_M p, int photographerId)
    {

        if (photographerId == 1)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(1, p.getPhotographerId());
            Assert.assertEquals("Amresh", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbum());
            AlbumBi_M_1_1_M album = p.getAlbum();
            Assert.assertNotNull(album);
            Assert.assertTrue(album.getAlbumId().equals("album_1"));
            Assert.assertEquals("My Phuket Vacation", album.getAlbumName());
            Assert.assertEquals("Went Phuket with friends", album.getAlbumDescription());

            List<PhotographerBi_M_1_1_M> ps = album.getPhotographers();
            Assert.assertNotNull(ps);
            Assert.assertFalse(ps.isEmpty());
            Assert.assertEquals(2, ps.size());

            List<PhotoBi_M_1_1_M> albumPhotos = album.getPhotos();
            Assert.assertNotNull(albumPhotos);
            Assert.assertFalse(albumPhotos.isEmpty());
            Assert.assertEquals(3, albumPhotos.size());

            PhotoBi_M_1_1_M photo1 = albumPhotos.get(0);
            PhotoBi_M_1_1_M photo2 = albumPhotos.get(1);
            PhotoBi_M_1_1_M photo3 = albumPhotos.get(2);

            Assert.assertNotNull(photo1);
            Assert.assertTrue(photo1.getPhotoId().startsWith("photo_"));
            Assert.assertNotNull(photo2);
            Assert.assertTrue(photo2.getPhotoId().startsWith("photo_"));
            Assert.assertNotNull(photo3);
            Assert.assertTrue(photo3.getPhotoId().startsWith("photo_"));

            Assert.assertNotNull(photo1.getAlbum());
            Assert.assertNotNull(photo2.getAlbum());
            Assert.assertNotNull(photo3.getAlbum());
        }
        else if (photographerId == 2)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(2, p.getPhotographerId());
            Assert.assertEquals("Vivek", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbum());
            AlbumBi_M_1_1_M album = p.getAlbum();
            Assert.assertNotNull(album);
            Assert.assertTrue(album.getAlbumId().equals("album_1"));
            Assert.assertEquals("My Phuket Vacation", album.getAlbumName());
            Assert.assertEquals("Went Phuket with friends", album.getAlbumDescription());

            List<PhotographerBi_M_1_1_M> ps = album.getPhotographers();
            Assert.assertNotNull(ps);
            Assert.assertFalse(ps.isEmpty());
            Assert.assertEquals(2, ps.size());

            List<PhotoBi_M_1_1_M> albumPhotos = album.getPhotos();
            Assert.assertNotNull(albumPhotos);
            Assert.assertFalse(albumPhotos.isEmpty());
            Assert.assertEquals(3, albumPhotos.size());

            PhotoBi_M_1_1_M photo1 = albumPhotos.get(0);
            PhotoBi_M_1_1_M photo2 = albumPhotos.get(1);
            PhotoBi_M_1_1_M photo3 = albumPhotos.get(2);

            Assert.assertNotNull(photo1);
            Assert.assertTrue(photo1.getPhotoId().startsWith("photo_"));
            Assert.assertNotNull(photo2);
            Assert.assertTrue(photo2.getPhotoId().startsWith("photo_"));
            Assert.assertNotNull(photo3);
            Assert.assertTrue(photo3.getPhotoId().startsWith("photo_"));

            Assert.assertNotNull(photo1.getAlbum());
            Assert.assertNotNull(photo2.getAlbum());
            Assert.assertNotNull(photo3.getAlbum());
        }
        else
        {
            Assert.fail("Invalid Photographer ID: " + photographerId);
        }

    }

    private void assertModifiedPhotographer(PhotographerBi_M_1_1_M p, int photographerId)
    {

        if (photographerId == 1)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(1, p.getPhotographerId());
            Assert.assertEquals("Amresh2", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbum());
            AlbumBi_M_1_1_M album = p.getAlbum();
            Assert.assertNotNull(album);
            Assert.assertTrue(album.getAlbumId().equals("album_1"));
            Assert.assertEquals("My Phuket Vacation", album.getAlbumName());
            Assert.assertEquals("Went Phuket with friends", album.getAlbumDescription());

            List<PhotographerBi_M_1_1_M> ps = album.getPhotographers();
            Assert.assertNotNull(ps);
            Assert.assertFalse(ps.isEmpty());
            Assert.assertEquals(2, ps.size());

            List<PhotoBi_M_1_1_M> albumPhotos = album.getPhotos();
            Assert.assertNotNull(albumPhotos);
            Assert.assertFalse(albumPhotos.isEmpty());
            Assert.assertEquals(3, albumPhotos.size());

            PhotoBi_M_1_1_M photo1 = albumPhotos.get(0);
            PhotoBi_M_1_1_M photo2 = albumPhotos.get(1);
            PhotoBi_M_1_1_M photo3 = albumPhotos.get(2);

            Assert.assertNotNull(photo1);
            Assert.assertTrue(photo1.getPhotoId().startsWith("photo_"));
            Assert.assertNotNull(photo2);
            Assert.assertTrue(photo2.getPhotoId().startsWith("photo_"));
            Assert.assertNotNull(photo3);
            Assert.assertTrue(photo3.getPhotoId().startsWith("photo_"));

            Assert.assertNotNull(photo1.getAlbum());
            Assert.assertNotNull(photo2.getAlbum());
            Assert.assertNotNull(photo3.getAlbum());
        }
        else if (photographerId == 2)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(2, p.getPhotographerId());
            Assert.assertEquals("Vivek2", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbum());
            AlbumBi_M_1_1_M album = p.getAlbum();
            Assert.assertNotNull(album);
            Assert.assertTrue(album.getAlbumId().equals("album_1"));
            Assert.assertEquals("My Phuket Vacation", album.getAlbumName());
            Assert.assertEquals("Went Phuket with friends", album.getAlbumDescription());

            List<PhotographerBi_M_1_1_M> ps = album.getPhotographers();
            Assert.assertNotNull(ps);
            Assert.assertFalse(ps.isEmpty());
            Assert.assertEquals(2, ps.size());

            List<PhotoBi_M_1_1_M> albumPhotos = album.getPhotos();
            Assert.assertNotNull(albumPhotos);
            Assert.assertFalse(albumPhotos.isEmpty());
            Assert.assertEquals(3, albumPhotos.size());

            PhotoBi_M_1_1_M photo1 = albumPhotos.get(0);
            PhotoBi_M_1_1_M photo2 = albumPhotos.get(1);
            PhotoBi_M_1_1_M photo3 = albumPhotos.get(2);

            Assert.assertNotNull(photo1);
            Assert.assertTrue(photo1.getPhotoId().startsWith("photo_"));
            Assert.assertNotNull(photo2);
            Assert.assertTrue(photo2.getPhotoId().startsWith("photo_"));
            Assert.assertNotNull(photo3);
            Assert.assertTrue(photo3.getPhotoId().startsWith("photo_"));

            Assert.assertNotNull(photo1.getAlbum());
            Assert.assertNotNull(photo2.getAlbum());
            Assert.assertNotNull(photo3.getAlbum());
        }
        else
        {
            Assert.fail("Invalid Photographer ID: " + photographerId);
        }

    }

    private List<PhotographerBi_M_1_1_M> populatePhotographers()
    {
        List<PhotographerBi_M_1_1_M> photographers = new ArrayList<PhotographerBi_M_1_1_M>();

        // Photographer 1
        PhotographerBi_M_1_1_M p1 = new PhotographerBi_M_1_1_M();
        p1.setPhotographerId(1);
        p1.setPhotographerName("Amresh");

        AlbumBi_M_1_1_M album = new AlbumBi_M_1_1_M("album_1", "My Phuket Vacation", "Went Phuket with friends");

        album.addPhoto(new PhotoBi_M_1_1_M("photo_1", "One beach", "On beach with friends"));
        album.addPhoto(new PhotoBi_M_1_1_M("photo_2", "In Hotel", "Chilling out in room"));
        album.addPhoto(new PhotoBi_M_1_1_M("photo_3", "At Airport", "So tired"));

        p1.setAlbum(album);

        // Photographer 2
        PhotographerBi_M_1_1_M p2 = new PhotographerBi_M_1_1_M();
        p2.setPhotographerId(2);
        p2.setPhotographerName("Vivek");

        p2.setAlbum(album);

        photographers.add(p1);
        photographers.add(p2);

        return photographers;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.tests.crossdatastore.pickr.PickrBaseTest#startServer
     * ()
     */
    @Override
    protected void startServer() throws IOException, TException, InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException
    {

        if (RUN_IN_EMBEDDED_MODE)
        {
            CassandraCli.cassandraSetUp();
//            HBaseCli.startCluster();
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
            ColumnDef pColumnDef5 = new ColumnDef(ByteBuffer.wrap("ALBUM_ID".getBytes()), "UTF8Type");
            pColumnDef5.index_type = IndexType.KEYS;
            pCfDef.addToColumn_metadata(pColumnDef2);
            pCfDef.addToColumn_metadata(pColumnDef5);

            CfDef aCfDef = new CfDef();
            aCfDef.name = "ALBUM";
            aCfDef.keyspace = "Pickr";
            aCfDef.setComparator_type("UTF8Type");
            aCfDef.setDefault_validation_class("UTF8Type");
            ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("ALBUM_NAME".getBytes()), "UTF8Type");
            columnDef.index_type = IndexType.KEYS;
            ColumnDef columnDef3 = new ColumnDef(ByteBuffer.wrap("ALBUM_DESC".getBytes()), "UTF8Type");
            columnDef3.index_type = IndexType.KEYS;

            aCfDef.addToColumn_metadata(columnDef);
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
            ColumnDef columnDef5 = new ColumnDef(ByteBuffer.wrap("ALBUM_ID".getBytes()), "UTF8Type");
            columnDef5.index_type = IndexType.KEYS;
            photoLinkCfDef.addToColumn_metadata(columnDef1);
            photoLinkCfDef.addToColumn_metadata(columnDef2);
            photoLinkCfDef.addToColumn_metadata(columnDef5);

            List<CfDef> cfDefs = new ArrayList<CfDef>();
            cfDefs.add(pCfDef);
            cfDefs.add(aCfDef);
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
                }
                CassandraCli.client.system_add_column_family(pCfDef);
                CassandraCli.client.system_add_column_family(aCfDef);
                CassandraCli.client.system_add_column_family(photoLinkCfDef);
            }
            catch (NotFoundException e)
            {
                ksDef = new KsDef("Pickr", "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
                ksDef.setReplication_factor(1);
                CassandraCli.client.system_add_keyspace(ksDef);
            }
            catch (InvalidRequestException e)
            {
                log.error(e.getMessage());
            }
            catch (TException e)
            {
                log.error(e.getMessage());
            }

            /**
             * schema generation for HBase.
             * */

//            HBaseCli.createTable("PHOTOGRAPHER");
//            HBaseCli.addColumnFamily("PHOTOGRAPHER", "PHOTOGRAPHER_NAME");
//            HBaseCli.addColumnFamily("PHOTOGRAPHER", "ALBUM_ID");
//
//            HBaseCli.createTable("ALBUM");
//            HBaseCli.addColumnFamily("ALBUM", "ALBUM_NAME");
//            HBaseCli.addColumnFamily("ALBUM", "ALBUM_DESC");
//
//            HBaseCli.createTable("PHOTO");
//            HBaseCli.addColumnFamily("PHOTO", "PHOTO_CAPTION");
//            HBaseCli.addColumnFamily("PHOTO", "PHOTO_DESC");
//            HBaseCli.addColumnFamily("PHOTO", "ALBUM_ID");
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.tests.crossdatastore.pickr.PickrBaseTest#stopServer()
     */
    @Override
    protected void stopServer()
    {
        HBaseCli.stopCluster();

    }

}
