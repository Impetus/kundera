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
import com.impetus.kundera.tests.crossdatastore.pickr.entities.album.AlbumUni_1_M_1_M;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.photo.PhotoUni_1_M_1_M;
import com.impetus.kundera.tests.crossdatastore.pickr.entities.photographer.PhotographerUni_1_M_1_M;

/**
 * @author amresh.singh
 * 
 */
public class PickrTestUni_1_M_1_M extends PickrBaseTest
{
    private static final Log log = LogFactory.getLog(PickrTestUni_1_M_1_M.class);;

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
        PhotographerUni_1_M_1_M p = preparePhotographer();

        pickr.addPhotographer(p);
    }

    @Override
    public void updatePhotographer()
    {
        PhotographerUni_1_M_1_M p = (PhotographerUni_1_M_1_M) pickr.getPhotographer(PhotographerUni_1_M_1_M.class, ""
                + photographerId);
        assertPhotographer(p);

        p.setPhotographerName("Vivek");

        pickr.mergePhotographer(p);

        PhotographerUni_1_M_1_M p2 = (PhotographerUni_1_M_1_M) pickr.getPhotographer(PhotographerUni_1_M_1_M.class, ""
                + photographerId);

        assertModifiedPhotographer(p2);
    }

    @Override
    public void getPhotographer()
    {
        PhotographerUni_1_M_1_M p = (PhotographerUni_1_M_1_M) pickr.getPhotographer(PhotographerUni_1_M_1_M.class, ""
                + photographerId);
        assertPhotographer(p);
    }

    @Override
    public void getAllPhotographers()
    {
        List<Object> ps = pickr.getAllPhotographers(PhotographerUni_1_M_1_M.class.getSimpleName());
        PhotographerUni_1_M_1_M p = (PhotographerUni_1_M_1_M) ps.get(0);

        assertModifiedPhotographer(p);

    }

    @Override
    public void deletePhotographer()
    {
        PhotographerUni_1_M_1_M p = (PhotographerUni_1_M_1_M) pickr.getPhotographer(PhotographerUni_1_M_1_M.class, ""
                + photographerId);
        assertModifiedPhotographer(p);
        pickr.deletePhotographer(p);
        PhotographerUni_1_M_1_M p2 = (PhotographerUni_1_M_1_M) pickr.getPhotographer(PhotographerUni_1_M_1_M.class, ""
                + photographerId);
        Assert.assertNull(p2);

    }

    private PhotographerUni_1_M_1_M preparePhotographer()
    {
        PhotographerUni_1_M_1_M p = new PhotographerUni_1_M_1_M();
        p.setPhotographerId(photographerId);
        p.setPhotographerName("Amresh");

        AlbumUni_1_M_1_M album1 = new AlbumUni_1_M_1_M("album_1", "My Phuket Vacation", "Went Phuket with friends");
        album1.addPhoto(new PhotoUni_1_M_1_M("photo_1", "One beach", "On beach with friends"));
        album1.addPhoto(new PhotoUni_1_M_1_M("photo_2", "In Hotel", "Chilling out in room"));
        album1.addPhoto(new PhotoUni_1_M_1_M("photo_3", "At Airport", "So tired"));

        AlbumUni_1_M_1_M album2 = new AlbumUni_1_M_1_M("album_2", "Office Pics", "Annual office party photos");
        album2.addPhoto(new PhotoUni_1_M_1_M("photo_4", "Office Team event", "Shot at Fun park"));
        album2.addPhoto(new PhotoUni_1_M_1_M("photo_5", "My Team", "My team is the best"));

        p.addAlbum(album1);
        p.addAlbum(album2);
        return p;

    }

    private void assertPhotographer(PhotographerUni_1_M_1_M p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals(1, p.getPhotographerId());
        Assert.assertEquals("Amresh", p.getPhotographerName());

        Assert.assertNotNull(p.getAlbums());
        Assert.assertFalse(p.getAlbums().isEmpty());
        Assert.assertEquals(2, p.getAlbums().size());

        AlbumUni_1_M_1_M album1 = p.getAlbums().get(0);
        Assert.assertNotNull(album1);
        Assert.assertTrue(album1.getAlbumId().equals("album_1") || album1.getAlbumId().equals("album_2"));

        Assert.assertFalse(album1.getAlbumName().length() == 0);
        Assert.assertFalse(album1.getAlbumDescription().length() == 0);

        List<PhotoUni_1_M_1_M> album1Photos = album1.getPhotos();
        Assert.assertNotNull(album1Photos);
        Assert.assertFalse(album1Photos.isEmpty());
        Assert.assertFalse(album1Photos.size() < 2);

        PhotoUni_1_M_1_M album1Photo1 = album1Photos.get(0);
        Assert.assertNotNull(album1Photo1);
        Assert.assertEquals(7, album1Photo1.getPhotoId().length());

        AlbumUni_1_M_1_M album2 = p.getAlbums().get(1);
        Assert.assertNotNull(album2);
        Assert.assertTrue(album2.getAlbumId().equals("album_1") || album2.getAlbumId().equals("album_2"));

        Assert.assertFalse(album2.getAlbumName().length() == 0);
        Assert.assertFalse(album2.getAlbumDescription().length() == 0);

        List<PhotoUni_1_M_1_M> album2Photos = album2.getPhotos();
        Assert.assertNotNull(album2Photos);
        Assert.assertFalse(album2Photos.isEmpty());
        Assert.assertFalse(album2Photos.size() < 2);

        PhotoUni_1_M_1_M album2Photo1 = album2Photos.get(0);
        Assert.assertNotNull(album2Photo1);
        Assert.assertEquals(7, album2Photo1.getPhotoId().length());
    }

    private void assertModifiedPhotographer(PhotographerUni_1_M_1_M p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals(1, p.getPhotographerId());
        Assert.assertEquals("Vivek", p.getPhotographerName());

        Assert.assertNotNull(p.getAlbums());
        Assert.assertFalse(p.getAlbums().isEmpty());
        Assert.assertEquals(2, p.getAlbums().size());

        AlbumUni_1_M_1_M album1 = p.getAlbums().get(0);
        Assert.assertNotNull(album1);
        Assert.assertTrue(album1.getAlbumId().equals("album_1") || album1.getAlbumId().equals("album_2"));

        Assert.assertFalse(album1.getAlbumName().length() == 0);
        Assert.assertFalse(album1.getAlbumDescription().length() == 0);

        List<PhotoUni_1_M_1_M> album1Photos = album1.getPhotos();
        Assert.assertNotNull(album1Photos);
        Assert.assertFalse(album1Photos.isEmpty());
        Assert.assertFalse(album1Photos.size() < 2);

        PhotoUni_1_M_1_M album1Photo1 = album1Photos.get(0);
        Assert.assertNotNull(album1Photo1);
        Assert.assertEquals(7, album1Photo1.getPhotoId().length());

        AlbumUni_1_M_1_M album2 = p.getAlbums().get(1);
        Assert.assertNotNull(album2);
        Assert.assertTrue(album2.getAlbumId().equals("album_1") || album2.getAlbumId().equals("album_2"));

        Assert.assertFalse(album2.getAlbumName().length() == 0);
        Assert.assertFalse(album2.getAlbumDescription().length() == 0);

        List<PhotoUni_1_M_1_M> album2Photos = album2.getPhotos();
        Assert.assertNotNull(album2Photos);
        Assert.assertFalse(album2Photos.isEmpty());
        Assert.assertFalse(album2Photos.size() < 2);

        PhotoUni_1_M_1_M album2Photo1 = album2Photos.get(0);
        Assert.assertNotNull(album2Photo1);
        Assert.assertEquals(7, album2Photo1.getPhotoId().length());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.tests.crossdatastore.pickr.PickrBaseTest#startServer
     * ()
     */
    @Override
    protected void startServer() throws IOException, TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
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
            ColumnDef columnDef5 = new ColumnDef(ByteBuffer.wrap("PHOTOGRAPHER_ID".getBytes()), "UTF8Type");
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
            ColumnDef columnDef4 = new ColumnDef(ByteBuffer.wrap("ALBUM_ID".getBytes()), "UTF8Type");
            columnDef4.index_type = IndexType.KEYS;
            photoLinkCfDef.addToColumn_metadata(columnDef1);
            photoLinkCfDef.addToColumn_metadata(columnDef2);
            photoLinkCfDef.addToColumn_metadata(columnDef4);

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
                CassandraCli.client.system_add_column_family(photoLinkCfDef);
                CassandraCli.client.system_add_column_family(aCfDef);
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

            /**
             * schema generation for cassandra.
             * */

//            HBaseCli.createTable("PHOTOGRAPHER");
//            HBaseCli.addColumnFamily("PHOTOGRAPHER", "PHOTOGRAPHER_NAME");
//
//            HBaseCli.createTable("PHOTO");
//            HBaseCli.addColumnFamily("PHOTO", "PHOTO_CAPTION");
//            HBaseCli.addColumnFamily("PHOTO", "PHOTO_DESC");
//            HBaseCli.addColumnFamily("PHOTO", "ALBUM_ID");
//
//            HBaseCli.createTable("ALBUM");
//            HBaseCli.addColumnFamily("ALBUM", "ALBUM_NAME");
//            HBaseCli.addColumnFamily("ALBUM", "ALBUM_DESC");
//            HBaseCli.addColumnFamily("ALBUM", "PHOTOGRAPHER_ID");
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
