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

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

import com.impetus.kundera.entity.photo.PhotoUni_M_M_M_M;

/**
 * Entity Class for album
 * 
 * @author amresh.singh
 */

@Entity
@Table(name = "ALBUM", schema = "KunderaTest@kunderatest")
public class AlbumUni_M_M_M_M
{
    @Id
    @Column(name = "ALBUM_ID")
    private String albumId;

    @Column(name = "ALBUM_NAME")
    private String albumName;

    @Column(name = "ALBUM_DESC")
    private String albumDescription;

    @ManyToMany(cascade = CascadeType.ALL)
    @JoinTable(name = "ALBUM_PHOTO", joinColumns = { @JoinColumn(name = "ALBUM_ID") }, inverseJoinColumns = { @JoinColumn(name = "PHOTO_ID") })
    private List<PhotoUni_M_M_M_M> photos;

    public AlbumUni_M_M_M_M()
    {

    }

    public AlbumUni_M_M_M_M(String albumId, String name, String description)
    {
        this.albumId = albumId;
        this.albumName = name;
        this.albumDescription = description;
    }

    public String getAlbumId()
    {
        return albumId;
    }

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
     * @return the photos
     */
    public List<PhotoUni_M_M_M_M> getPhotos()
    {
        if (this.photos == null || this.photos.isEmpty())
        {
            this.photos = new ArrayList<PhotoUni_M_M_M_M>();
        }
        return photos;
    }

    /**
     * @param photos
     *            the photos to set
     */
    public void setPhotos(List<PhotoUni_M_M_M_M> photos)
    {
        this.photos = photos;
    }

    public void addPhoto(PhotoUni_M_M_M_M photo)
    {
        getPhotos().add(photo);
    }

}
