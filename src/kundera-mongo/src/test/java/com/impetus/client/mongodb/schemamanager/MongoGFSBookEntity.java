/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.mongodb.schemamanager;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;

/**
 * The Class MongoGFSBookEntity.
 * 
 * @author Devender Yadav
 */
@Entity
@Table(name = "GFS_BOOK", schema = "KunderaMongoSchemaGeneration@mongoSchemaGenerationTest")
public class MongoGFSBookEntity
{

    /** The id. */
    @Id
    private String id;

    /** The title. */
    @Column
    private String title;

    /** The pdf file. */
    @Lob
    private byte[] pdfFile;

    /**
     * Gets the id.
     * 
     * @return the id
     */
    public String getId()
    {
        return id;
    }

    /**
     * Sets the id.
     * 
     * @param id
     *            the new id
     */
    public void setId(String id)
    {
        this.id = id;
    }

    /**
     * Gets the title.
     * 
     * @return the title
     */
    public String getTitle()
    {
        return title;
    }

    /**
     * Sets the title.
     * 
     * @param title
     *            the new title
     */
    public void setTitle(String title)
    {
        this.title = title;
    }

    /**
     * Gets the pdf file.
     * 
     * @return the pdf file
     */
    public byte[] getPdfFile()
    {
        return pdfFile;
    }

    /**
     * Sets the pdf file.
     * 
     * @param pdfFile
     *            the new pdf file
     */
    public void setPdfFile(byte[] pdfFile)
    {
        this.pdfFile = pdfFile;
    }

}
