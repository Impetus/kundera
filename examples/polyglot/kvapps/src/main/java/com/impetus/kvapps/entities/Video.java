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
package com.impetus.kvapps.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author impetus
 * 
 *  Video entity
 */
@Entity
@Table(name = "video")
public class Video {

	@Id
	@Column(name = "video_id")
	private String videoId;

	@Column(name = "video_name")
	private String videoName;

	@Column(name = "video_provider")
	private String videoProvider;

	public Video() {
		// Default constructor.
	}

	public String getVideoId() {
		return videoId;
	}

	public void setVideoId(String videoId) {
		this.videoId = videoId;
	}

	public String getVideoName() {
		return videoName;
	}

	public void setVideoName(String videoName) {
		this.videoName = videoName;
	}

	public String getVideoProvider() {
		return videoProvider;
	}

	public void setVideoProvider(String videoProvider) {
		this.videoProvider = videoProvider;
	}

	@Override
	public String toString() {
		return "Video [videoId=" + videoId + ", videoName=" + videoName
				+ ", videoProvider=" + videoProvider + "]";
	}

}
