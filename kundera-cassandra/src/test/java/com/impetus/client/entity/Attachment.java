/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.client.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Entity class for Email attachment.
 * 
 * @author amresh.singh
 */

@Entity
@Table(name = "attachments", schema = "Blog")
public class Attachment
{

    /** The attachment id. */
    @Id
    private String attachmentId;

    /** The file name. */
    @Column(name = "file_name")
    private String fileName;

    /** The file type. */
    @Column(name = "file_type")
    private String fileType;

    /** The file location. */
    @Column(name = "file_location")
    private String fileLocation;

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    public String toString()
    {
        return "ID: " + attachmentId + "\tName: " + fileName + "\tType: " + fileType + "\tLocation: " + fileLocation;
    }

    /**
     * Gets the attachment id.
     * 
     * @return the attachmentId
     */
    public String getAttachmentId()
    {
        return attachmentId;
    }

    /**
     * Sets the attachment id.
     * 
     * @param attachmentId
     *            the attachmentId to set
     */
    public void setAttachmentId(String attachmentId)
    {
        this.attachmentId = attachmentId;
    }

    /**
     * Gets the file name.
     * 
     * @return the fileName
     */
    public String getFileName()
    {
        return fileName;
    }

    /**
     * Sets the file name.
     * 
     * @param fileName
     *            the fileName to set
     */
    public void setFileName(String fileName)
    {
        this.fileName = fileName;
    }

    /**
     * Gets the file type.
     * 
     * @return the fileType
     */
    public String getFileType()
    {
        return fileType;
    }

    /**
     * Sets the file type.
     * 
     * @param fileType
     *            the fileType to set
     */
    public void setFileType(String fileType)
    {
        this.fileType = fileType;
    }

    /**
     * Gets the file location.
     * 
     * @return the fileLocation
     */
    public String getFileLocation()
    {
        return fileLocation;
    }

    /**
     * Sets the file location.
     * 
     * @param fileLocation
     *            the fileLocation to set
     */
    public void setFileLocation(String fileLocation)
    {
        this.fileLocation = fileLocation;
    }
}
