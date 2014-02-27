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
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.album.AlbumBi_M_1_M_1;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.photo.PhotoBi_M_1_M_1;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.photographer.PhotographerBi_M_1_M_1;

/**
 * @author amresh.singh
 * 
 */
public class PickrTestBi_M_1_M_1 extends PickrBaseTest
{
    private static Logger log = LoggerFactory.getLogger(PickrTestBi_M_1_M_1.class);

    @Before
    public void setUp() throws Exception
    {
        log.info("Executing PICKR Test: " + this.getClass().getSimpleName() + "\n======"
                + "==========================================================");
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
        List<PhotographerBi_M_1_M_1> ps = populatePhotographers();

        for (PhotographerBi_M_1_M_1 p : ps)
        {
            pickr.addPhotographer(p);
        }
    }

    @Override
    protected void getPhotographer()
    {
        PhotographerBi_M_1_M_1 p1 = (PhotographerBi_M_1_M_1) pickr.getPhotographer(PhotographerBi_M_1_M_1.class, 1);
        assertPhotographer(p1, 1);

        PhotographerBi_M_1_M_1 p2 = (PhotographerBi_M_1_M_1) pickr.getPhotographer(PhotographerBi_M_1_M_1.class, 2);
        assertPhotographer(p2, 2);

        PhotographerBi_M_1_M_1 p3 = (PhotographerBi_M_1_M_1) pickr.getPhotographer(PhotographerBi_M_1_M_1.class, 3);
        assertPhotographer(p3, 3);

        PhotographerBi_M_1_M_1 p4 = (PhotographerBi_M_1_M_1) pickr.getPhotographer(PhotographerBi_M_1_M_1.class, 4);
        assertPhotographer(p4, 4);
    }

    @Override
    protected void updatePhotographer()
    {
        PhotographerBi_M_1_M_1 p1 = (PhotographerBi_M_1_M_1) pickr.getPhotographer(PhotographerBi_M_1_M_1.class, 1);
        assertPhotographer(p1, 1);
        p1.setPhotographerName("Amresh2");

        pickr.mergePhotographer(p1);

        PhotographerBi_M_1_M_1 p1Modified = (PhotographerBi_M_1_M_1) pickr.getPhotographer(
                PhotographerBi_M_1_M_1.class, 1);
        assertModifiedPhotographer(p1Modified, 1);

        PhotographerBi_M_1_M_1 p2 = (PhotographerBi_M_1_M_1) pickr.getPhotographer(PhotographerBi_M_1_M_1.class, 2);
        assertPhotographer(p2, 2);

        p2.setPhotographerName("Vivek2");
        pickr.mergePhotographer(p2);

        PhotographerBi_M_1_M_1 p2Modified = (PhotographerBi_M_1_M_1) pickr.getPhotographer(
                PhotographerBi_M_1_M_1.class, 2);
        assertModifiedPhotographer(p2Modified, 2);

        PhotographerBi_M_1_M_1 p3 = (PhotographerBi_M_1_M_1) pickr.getPhotographer(PhotographerBi_M_1_M_1.class, 3);
        assertPhotographer(p3, 3);
        p3.setPhotographerName("Kuldeep2");

        pickr.mergePhotographer(p3);

        PhotographerBi_M_1_M_1 p3Modified = (PhotographerBi_M_1_M_1) pickr.getPhotographer(
                PhotographerBi_M_1_M_1.class, 3);
        assertModifiedPhotographer(p3Modified, 3);

        PhotographerBi_M_1_M_1 p4 = (PhotographerBi_M_1_M_1) pickr.getPhotographer(PhotographerBi_M_1_M_1.class, 4);
        assertPhotographer(p4, 4);

        p4.setPhotographerName("VivekS2");
        pickr.mergePhotographer(p4);

        PhotographerBi_M_1_M_1 p4Modified = (PhotographerBi_M_1_M_1) pickr.getPhotographer(
                PhotographerBi_M_1_M_1.class, 4);
        assertModifiedPhotographer(p4Modified, 4);
    }

    @Override
    protected void getAllPhotographers()
    {
        List<Object> ps = pickr.getAllPhotographers(PhotographerBi_M_1_M_1.class.getSimpleName());

        for (Object p : ps)
        {
            PhotographerBi_M_1_M_1 pp = (PhotographerBi_M_1_M_1) p;
            Assert.assertNotNull(pp);
            assertModifiedPhotographer(pp, pp.getPhotographerId());
        }

    }

    @Override
    protected void deletePhotographer()
    {
        PhotographerBi_M_1_M_1 p1 = (PhotographerBi_M_1_M_1) pickr.getPhotographer(PhotographerBi_M_1_M_1.class, 1);
        assertModifiedPhotographer(p1, 1);

        pickr.deletePhotographer(p1);

        PhotographerBi_M_1_M_1 p1AfterDeletion = (PhotographerBi_M_1_M_1) pickr.getPhotographer(
                PhotographerBi_M_1_M_1.class, 1);
        Assert.assertNull(p1AfterDeletion);

        PhotographerBi_M_1_M_1 p2 = (PhotographerBi_M_1_M_1) pickr.getPhotographer(PhotographerBi_M_1_M_1.class, 2);
        Assert.assertNotNull(p2);

        pickr.deletePhotographer(p2);

        PhotographerBi_M_1_M_1 p2AfterDeletion = (PhotographerBi_M_1_M_1) pickr.getPhotographer(
                PhotographerBi_M_1_M_1.class, 2);
        Assert.assertNull(p2AfterDeletion);

        PhotographerBi_M_1_M_1 p3 = (PhotographerBi_M_1_M_1) pickr.getPhotographer(PhotographerBi_M_1_M_1.class, 3);
        Assert.assertNotNull(p3);

        pickr.deletePhotographer(p3);

        PhotographerBi_M_1_M_1 p3AfterDeletion = (PhotographerBi_M_1_M_1) pickr.getPhotographer(
                PhotographerBi_M_1_M_1.class, 3);
        Assert.assertNull(p3AfterDeletion);

        PhotographerBi_M_1_M_1 p4 = (PhotographerBi_M_1_M_1) pickr.getPhotographer(PhotographerBi_M_1_M_1.class, 4);
        Assert.assertNotNull(p4);

        pickr.deletePhotographer(p4);

        PhotographerBi_M_1_M_1 p4AfterDeletion = (PhotographerBi_M_1_M_1) pickr.getPhotographer(
                PhotographerBi_M_1_M_1.class, 4);
        Assert.assertNull(p4AfterDeletion);

    }

    private void assertPhotographer(PhotographerBi_M_1_M_1 p, int photographerId)
    {

        if (photographerId == 1)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(1, p.getPhotographerId());
            Assert.assertEquals("Amresh", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbum());
            AlbumBi_M_1_M_1 album = p.getAlbum();
            Assert.assertNotNull(album);
            Assert.assertTrue(album.getAlbumId().equals("album_1"));
            Assert.assertEquals("My Phuket Vacation", album.getAlbumName());
            Assert.assertEquals("Went Phuket with friends", album.getAlbumDescription());

            Assert.assertNotNull(album.getPhotographers());
            Assert.assertEquals(2, album.getPhotographers().size());
            Assert.assertNotNull(album.getPhotographers().get(0));
            Assert.assertNotNull(album.getPhotographers().get(1));

            PhotoBi_M_1_M_1 albumPhoto = album.getPhoto();
            Assert.assertNotNull(albumPhoto);

            Assert.assertNotNull(albumPhoto);
            Assert.assertTrue(albumPhoto.getPhotoId().equals("photo_1"));

            Assert.assertNotNull(albumPhoto.getAlbums());
            Assert.assertEquals(2, albumPhoto.getAlbums().size());
            Assert.assertNotNull(albumPhoto.getAlbums().get(0));
            Assert.assertNotNull(albumPhoto.getAlbums().get(1));

        }
        else if (photographerId == 2)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(2, p.getPhotographerId());
            Assert.assertEquals("Vivek", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbum());
            AlbumBi_M_1_M_1 album = p.getAlbum();
            Assert.assertNotNull(album);
            Assert.assertTrue(album.getAlbumId().equals("album_1"));
            Assert.assertEquals("My Phuket Vacation", album.getAlbumName());
            Assert.assertEquals("Went Phuket with friends", album.getAlbumDescription());

            Assert.assertNotNull(album.getPhotographers());
            Assert.assertEquals(2, album.getPhotographers().size());
            Assert.assertNotNull(album.getPhotographers().get(0));
            Assert.assertNotNull(album.getPhotographers().get(1));

            PhotoBi_M_1_M_1 albumPhoto = album.getPhoto();
            Assert.assertNotNull(albumPhoto);

            Assert.assertNotNull(albumPhoto);
            Assert.assertTrue(albumPhoto.getPhotoId().equals("photo_1"));

            Assert.assertNotNull(albumPhoto.getAlbums());
            Assert.assertEquals(2, albumPhoto.getAlbums().size());
            Assert.assertNotNull(albumPhoto.getAlbums().get(0));
            Assert.assertNotNull(albumPhoto.getAlbums().get(1));
        }
        else if (photographerId == 3)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(3, p.getPhotographerId());
            Assert.assertEquals("Kuldeep", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbum());
            AlbumBi_M_1_M_1 album = p.getAlbum();
            Assert.assertNotNull(album);
            Assert.assertTrue(album.getAlbumId().equals("album_2"));
            Assert.assertEquals("My Shimla Vacation", album.getAlbumName());
            Assert.assertEquals("Went Shimla with friends", album.getAlbumDescription());

            Assert.assertNotNull(album.getPhotographers());
            Assert.assertEquals(2, album.getPhotographers().size());
            Assert.assertNotNull(album.getPhotographers().get(0));
            Assert.assertNotNull(album.getPhotographers().get(1));

            PhotoBi_M_1_M_1 albumPhoto = album.getPhoto();
            Assert.assertNotNull(albumPhoto);

            Assert.assertNotNull(albumPhoto);
            Assert.assertTrue(albumPhoto.getPhotoId().equals("photo_1"));

            Assert.assertNotNull(albumPhoto.getAlbums());
            Assert.assertEquals(2, albumPhoto.getAlbums().size());
            Assert.assertNotNull(albumPhoto.getAlbums().get(0));
            Assert.assertNotNull(albumPhoto.getAlbums().get(1));
        }
        else if (photographerId == 4)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(4, p.getPhotographerId());
            Assert.assertEquals("VivekS", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbum());
            AlbumBi_M_1_M_1 album = p.getAlbum();
            Assert.assertNotNull(album);
            Assert.assertTrue(album.getAlbumId().equals("album_2"));
            Assert.assertEquals("My Shimla Vacation", album.getAlbumName());
            Assert.assertEquals("Went Shimla with friends", album.getAlbumDescription());

            Assert.assertNotNull(album.getPhotographers());
            Assert.assertEquals(2, album.getPhotographers().size());
            Assert.assertNotNull(album.getPhotographers().get(0));
            Assert.assertNotNull(album.getPhotographers().get(1));

            PhotoBi_M_1_M_1 albumPhoto = album.getPhoto();
            Assert.assertNotNull(albumPhoto);

            Assert.assertNotNull(albumPhoto);
            Assert.assertTrue(albumPhoto.getPhotoId().equals("photo_1"));

            Assert.assertNotNull(albumPhoto.getAlbums());
            Assert.assertEquals(2, albumPhoto.getAlbums().size());
            Assert.assertNotNull(albumPhoto.getAlbums().get(0));
            Assert.assertNotNull(albumPhoto.getAlbums().get(1));
        }
        else
        {
            Assert.fail("Invalid Photographer ID: " + photographerId);
        }

    }

    private void assertModifiedPhotographer(PhotographerBi_M_1_M_1 p, int photographerId)
    {

        if (photographerId == 1)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(1, p.getPhotographerId());
            Assert.assertEquals("Amresh2", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbum());
            AlbumBi_M_1_M_1 album = p.getAlbum();
            Assert.assertNotNull(album);
            Assert.assertTrue(album.getAlbumId().equals("album_1"));
            Assert.assertEquals("My Phuket Vacation", album.getAlbumName());
            Assert.assertEquals("Went Phuket with friends", album.getAlbumDescription());

            Assert.assertNotNull(album.getPhotographers());
            Assert.assertEquals(2, album.getPhotographers().size());
            Assert.assertNotNull(album.getPhotographers().get(0));
            Assert.assertNotNull(album.getPhotographers().get(1));

            PhotoBi_M_1_M_1 albumPhoto = album.getPhoto();
            Assert.assertNotNull(albumPhoto);

            Assert.assertNotNull(albumPhoto);
            Assert.assertTrue(albumPhoto.getPhotoId().equals("photo_1"));

            Assert.assertNotNull(albumPhoto.getAlbums());
            Assert.assertEquals(2, albumPhoto.getAlbums().size());
            Assert.assertNotNull(albumPhoto.getAlbums().get(0));
            Assert.assertNotNull(albumPhoto.getAlbums().get(1));
        }
        else if (photographerId == 2)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(2, p.getPhotographerId());
            Assert.assertEquals("Vivek2", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbum());
            AlbumBi_M_1_M_1 album = p.getAlbum();
            Assert.assertNotNull(album);
            Assert.assertTrue(album.getAlbumId().equals("album_1"));
            Assert.assertEquals("My Phuket Vacation", album.getAlbumName());
            Assert.assertEquals("Went Phuket with friends", album.getAlbumDescription());

            Assert.assertNotNull(album.getPhotographers());
            Assert.assertEquals(2, album.getPhotographers().size());
            Assert.assertNotNull(album.getPhotographers().get(0));
            Assert.assertNotNull(album.getPhotographers().get(1));

            PhotoBi_M_1_M_1 albumPhoto = album.getPhoto();
            Assert.assertNotNull(albumPhoto);

            Assert.assertNotNull(albumPhoto);
            Assert.assertTrue(albumPhoto.getPhotoId().equals("photo_1"));

            Assert.assertNotNull(albumPhoto.getAlbums());
            Assert.assertEquals(2, albumPhoto.getAlbums().size());
            Assert.assertNotNull(albumPhoto.getAlbums().get(0));
            Assert.assertNotNull(albumPhoto.getAlbums().get(1));
        }
        else if (photographerId == 3)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(3, p.getPhotographerId());
            Assert.assertEquals("Kuldeep2", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbum());
            AlbumBi_M_1_M_1 album = p.getAlbum();
            Assert.assertNotNull(album);
            Assert.assertTrue(album.getAlbumId().equals("album_2"));
            Assert.assertEquals("My Shimla Vacation", album.getAlbumName());
            Assert.assertEquals("Went Shimla with friends", album.getAlbumDescription());

            Assert.assertNotNull(album.getPhotographers());
            Assert.assertEquals(2, album.getPhotographers().size());
            Assert.assertNotNull(album.getPhotographers().get(0));
            Assert.assertNotNull(album.getPhotographers().get(1));

            PhotoBi_M_1_M_1 albumPhoto = album.getPhoto();
            Assert.assertNotNull(albumPhoto);

            Assert.assertNotNull(albumPhoto);
            Assert.assertTrue(albumPhoto.getPhotoId().equals("photo_1"));

            Assert.assertNotNull(albumPhoto.getAlbums());
            Assert.assertEquals(2, albumPhoto.getAlbums().size());
            Assert.assertNotNull(albumPhoto.getAlbums().get(0));
            Assert.assertNotNull(albumPhoto.getAlbums().get(1));
        }
        else if (photographerId == 4)
        {
            Assert.assertNotNull(p);
            Assert.assertEquals(4, p.getPhotographerId());
            Assert.assertEquals("VivekS2", p.getPhotographerName());

            Assert.assertNotNull(p.getAlbum());
            AlbumBi_M_1_M_1 album = p.getAlbum();
            Assert.assertNotNull(album);
            Assert.assertTrue(album.getAlbumId().equals("album_2"));
            Assert.assertEquals("My Shimla Vacation", album.getAlbumName());
            Assert.assertEquals("Went Shimla with friends", album.getAlbumDescription());

            Assert.assertNotNull(album.getPhotographers());
            Assert.assertEquals(2, album.getPhotographers().size());
            Assert.assertNotNull(album.getPhotographers().get(0));
            Assert.assertNotNull(album.getPhotographers().get(1));

            PhotoBi_M_1_M_1 albumPhoto = album.getPhoto();
            Assert.assertNotNull(albumPhoto);

            Assert.assertNotNull(albumPhoto);
            Assert.assertTrue(albumPhoto.getPhotoId().equals("photo_1"));

            Assert.assertNotNull(albumPhoto.getAlbums());
            Assert.assertEquals(2, albumPhoto.getAlbums().size());
            Assert.assertNotNull(albumPhoto.getAlbums().get(0));
            Assert.assertNotNull(albumPhoto.getAlbums().get(1));
        }
        else
        {
            Assert.fail("Invalid Photographer ID: " + photographerId);
        }

    }

    private List<PhotographerBi_M_1_M_1> populatePhotographers()
    {
        List<PhotographerBi_M_1_M_1> photographers = new ArrayList<PhotographerBi_M_1_M_1>();

        // Photographer 1
        PhotographerBi_M_1_M_1 p1 = new PhotographerBi_M_1_M_1();
        p1.setPhotographerId(1);
        p1.setPhotographerName("Amresh");

        AlbumBi_M_1_M_1 album1 = new AlbumBi_M_1_M_1("album_1", "My Phuket Vacation", "Went Phuket with friends");

        AlbumBi_M_1_M_1 album2 = new AlbumBi_M_1_M_1("album_2", "My Shimla Vacation", "Went Shimla with friends");

        PhotoBi_M_1_M_1 photo = new PhotoBi_M_1_M_1("photo_1", "One beach", "On beach with friends");
        album1.setPhoto(photo);
        album2.setPhoto(photo);

        p1.setAlbum(album1);

        // Photographer 2
        PhotographerBi_M_1_M_1 p2 = new PhotographerBi_M_1_M_1();
        p2.setPhotographerId(2);
        p2.setPhotographerName("Vivek");

        p2.setAlbum(album1);

        // Photographer 3
        PhotographerBi_M_1_M_1 p3 = new PhotographerBi_M_1_M_1();
        p3.setPhotographerId(3);
        p3.setPhotographerName("Kuldeep");

        p3.setAlbum(album2);

        // Photographer 4
        PhotographerBi_M_1_M_1 p4 = new PhotographerBi_M_1_M_1();
        p4.setPhotographerId(4);
        p4.setPhotographerName("VivekS");

        p4.setAlbum(album2);

        photographers.add(p1);
        photographers.add(p2);
        photographers.add(p3);
        photographers.add(p4);

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
    protected void createCassandraSchema() throws IOException, TException, InvalidRequestException,
            UnavailableException, TimedOutException, SchemaDisagreementException
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
        ColumnDef columnDef5 = new ColumnDef(ByteBuffer.wrap("PHOTO_ID".getBytes()), "UTF8Type");
        columnDef5.index_type = IndexType.KEYS;

        aCfDef.addToColumn_metadata(columnDef);
        aCfDef.addToColumn_metadata(columnDef3);
        aCfDef.addToColumn_metadata(columnDef5);

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

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(pCfDef);
        cfDefs.add(aCfDef);
        cfDefs.add(photoLinkCfDef);
        try
        {
            ksDef = CassandraCli.client.describe_keyspace("Pickr");
            CassandraCli.client.set_keyspace("Pickr");
            if (!CassandraCli.columnFamilyExist("PHOTOGRAPHER", "Pickr")) {
                CassandraCli.client.system_add_column_family(pCfDef);
            } else {
                CassandraCli.truncateColumnFamily("Pickr", "PHOTOGRAPHER");
            }
            if (!CassandraCli.columnFamilyExist("ALBUM", "Pickr")) {
                CassandraCli.client.system_add_column_family(photoLinkCfDef);
            } else {
                CassandraCli.truncateColumnFamily("Pickr", "ALBUM");
            }
            if (!CassandraCli.columnFamilyExist("PHOTO", "Pickr")) {
                CassandraCli.client.system_add_column_family(aCfDef);
            } else {
                CassandraCli.truncateColumnFamily("Pickr", "PHOTO");
            }
         

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
