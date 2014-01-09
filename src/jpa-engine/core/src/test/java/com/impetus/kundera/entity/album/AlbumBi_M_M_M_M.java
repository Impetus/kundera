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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

import com.impetus.kundera.entity.photo.PhotoBi_M_M_M_M;
import com.impetus.kundera.entity.photographer.PhotographerBi_M_M_M_M;

/**
 * Entity Class for album
 * 
 * @author amresh.singh
 */

@Entity
@Table(name = "ALBUM", schema = "KunderaTest@kunderatest")
public class AlbumBi_M_M_M_M
{
    @Id
    @Column(name = "ALBUM_ID")
    private String albumId;

    @Column(name = "ALBUM_NAME")
    private String albumName;

    @Column(name = "ALBUM_DESC")
    private String albumDescription;

    @ManyToMany
    @JoinTable(name = "ALBUM_PHOTO", joinColumns = { @JoinColumn(name = "ALBUM_ID") }, inverseJoinColumns = { @JoinColumn(name = "PHOTO_ID") })
    private List<PhotoBi_M_M_M_M> photos;

    @ManyToMany(mappedBy = "albums", fetch = FetchType.LAZY)
    private List<PhotographerBi_M_M_M_M> photographers;

    public AlbumBi_M_M_M_M()
    {

    }

    public AlbumBi_M_M_M_M(String albumId, String name, String description)
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
    public List<PhotoBi_M_M_M_M> getPhotos()
    {
        if (this.photos == null || this.photos.isEmpty())
        {
            this.photos = new ArrayList<PhotoBi_M_M_M_M>();
        }
        return photos;
    }

    /**
     * @param photos
     *            the photos to set
     */
    public void setPhotos(List<PhotoBi_M_M_M_M> photos)
    {
        this.photos = photos;
    }

    public void addPhoto(PhotoBi_M_M_M_M photo)
    {
        getPhotos().add(photo);
    }

    /**
     * @return the photographers
     */
    public List<PhotographerBi_M_M_M_M> getPhotographers()
    {
        return photographers;
    }

    /**
     * @param photographers
     *            the photographers to set
     */
    public void setPhotographers(List<PhotographerBi_M_M_M_M> photographers)
    {
        this.photographers = photographers;
    }

}
