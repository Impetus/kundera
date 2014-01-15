/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.entity.album;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import com.impetus.kundera.entity.photo.PhotoBi_1_1_1_M;
import com.impetus.kundera.entity.photographer.PhotographerBi_1_1_1_M;

/**
 * Entity Class for album
 * 
 * @author amresh.singh
 */

@Entity
@Table(name = "ALBUM", schema = "KunderaTest@kunderatest")
public class AlbumBi_1_1_1_M
{
    @Id
    @Column(name = "ALBUM_ID")
    private String albumId;

    @Column(name = "ALBUM_NAME")
    private String albumName;

    @Column(name = "ALBUM_DESC")
    private String albumDescription;

    // One to many, will be persisted separately
    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY, mappedBy = "album")
    private List<PhotoBi_1_1_1_M> photos;

    @OneToOne(mappedBy = "album")
    private PhotographerBi_1_1_1_M photographer;

    public AlbumBi_1_1_1_M()
    {

    }

    public AlbumBi_1_1_1_M(String albumId, String name, String description)
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
    public List<PhotoBi_1_1_1_M> getPhotos()
    {
        if (photos == null)
        {
            photos = new ArrayList<PhotoBi_1_1_1_M>();
        }
        return photos;
    }

    /**
     * @param photos
     *            the photos to set
     */
    public void setPhotos(List<PhotoBi_1_1_1_M> photos)
    {
        this.photos = photos;
    }

    public void addPhoto(PhotoBi_1_1_1_M photo)
    {
        getPhotos().add(photo);
    }

    /**
     * @return the photographer
     */
    public PhotographerBi_1_1_1_M getPhotographer()
    {
        return photographer;
    }

    /**
     * @param photographer
     *            the photographer to set
     */
    public void setPhotographer(PhotographerBi_1_1_1_M photographer)
    {
        this.photographer = photographer;
    }

}
