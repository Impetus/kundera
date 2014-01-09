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
package com.impetus.kundera.entity.album;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import com.impetus.kundera.entity.photo.PhotoBi_1_1_1_1;
import com.impetus.kundera.entity.photographer.PhotographerBi_1_1_1_1;

/**
 * @author amresh.singh
 * 
 */
@Entity
@Table(name = "ALBUM", schema = "KunderaTest@kunderatest")
public class AlbumBi_1_1_1_1
{
    @Id
    @Column(name = "ALBUM_ID")
    private String albumId;

    @Column(name = "ALBUM_NAME")
    private String albumName;

    @Column(name = "ALBUM_DESC")
    private String albumDescription;

    // One to many, will be persisted separately
    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "PHOTO_ID")
    private PhotoBi_1_1_1_1 photo;

    @OneToOne(mappedBy = "album")
    private PhotographerBi_1_1_1_1 photographer;

    public AlbumBi_1_1_1_1()
    {

    }

    public AlbumBi_1_1_1_1(String albumId, String name, String description)
    {
        this.albumId = albumId;
        this.albumName = name;
        this.albumDescription = description;
    }

    /**
     * @return the albumId
     */
    public String getAlbumId()
    {
        return albumId;
    }

    /**
     * @param albumId
     *            the albumId to set
     */
    public void setAlbumId(String albumId)
    {
        this.albumId = albumId;
    }

    /**
     * @return the albumName
     */
    public String getAlbumName()
    {
        return albumName;
    }

    /**
     * @param albumName
     *            the albumName to set
     */
    public void setAlbumName(String albumName)
    {
        this.albumName = albumName;
    }

    /**
     * @return the albumDescription
     */
    public String getAlbumDescription()
    {
        return albumDescription;
    }

    /**
     * @param albumDescription
     *            the albumDescription to set
     */
    public void setAlbumDescription(String albumDescription)
    {
        this.albumDescription = albumDescription;
    }

    /**
     * @return the photo
     */
    public PhotoBi_1_1_1_1 getPhoto()
    {
        return photo;
    }

    /**
     * @param photo
     *            the photo to set
     */
    public void setPhoto(PhotoBi_1_1_1_1 photo)
    {
        this.photo = photo;
    }

    /**
     * @return the photographer
     */
    public PhotographerBi_1_1_1_1 getPhotographer()
    {
        return photographer;
    }

    /**
     * @param photographer
     *            the photographer to set
     */
    public void setPhotographer(PhotographerBi_1_1_1_1 photographer)
    {
        this.photographer = photographer;
    }

}