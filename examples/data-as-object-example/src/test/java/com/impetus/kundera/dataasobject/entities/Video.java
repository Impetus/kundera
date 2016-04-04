/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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
package com.impetus.kundera.dataasobject.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import com.impetus.core.DefaultKunderaEntity;

/**
 * The Class Video.
 *
 * @author impetus
 * 
 *         Video entity
 */
@Entity
public class Video extends DefaultKunderaEntity<Video, String>
{

    /** The video id. */
    @Id
    @Column(name = "video_id")
    private String videoId;

    /** The video name. */
    @Column(name = "video_name")
    private String videoName;

    /** The video provider. */
    @Column(name = "video_provider")
    private String videoProvider;

    /**
     * Instantiates a new video.
     */
    public Video()
    {
        // Default constructor.
    }

    /**
     * Gets the video id.
     *
     * @return the video id
     */
    public String getVideoId()
    {
        return videoId;
    }

    /**
     * Sets the video id.
     *
     * @param videoId
     *            the new video id
     */
    public void setVideoId(String videoId)
    {
        this.videoId = videoId;
    }

    /**
     * Gets the video name.
     *
     * @return the video name
     */
    public String getVideoName()
    {
        return videoName;
    }

    /**
     * Sets the video name.
     *
     * @param videoName
     *            the new video name
     */
    public void setVideoName(String videoName)
    {
        this.videoName = videoName;
    }

    /**
     * Gets the video provider.
     *
     * @return the video provider
     */
    public String getVideoProvider()
    {
        return videoProvider;
    }

    /**
     * Sets the video provider.
     *
     * @param videoProvider
     *            the new video provider
     */
    public void setVideoProvider(String videoProvider)
    {
        this.videoProvider = videoProvider;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return "Video [videoId=" + videoId + ", videoName=" + videoName + ", videoProvider=" + videoProvider + "]";
    }

}
