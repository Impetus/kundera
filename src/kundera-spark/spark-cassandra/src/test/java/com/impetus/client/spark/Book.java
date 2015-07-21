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
package com.impetus.client.spark;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class Book.
 * 
 * @author devender.yadav
 */
@Entity
@Table(name = "spark_book")
public class Book implements Serializable
{

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The id. */
    @Id
    private String id;

    /** The title. */
    @Column
    private String title;

    /** The author. */
    @Column
    private String author;

    /** The category. */
    @Column
    private String category;

    /** The num pages. */
    @Column
    private int numPages;

    /**
     * Instantiates a new book.
     */
    public Book()
    {

    }

    /**
     * Instantiates a new book.
     * 
     * @param id
     *            the id
     * @param title
     *            the title
     * @param author
     *            the author
     * @param category
     *            the category
     * @param numPages
     *            the num pages
     */
    public Book(String id, String title, String author, String category, int numPages)
    {
        this.id = id;
        this.title = title;
        this.author = author;
        this.category = category;
        this.numPages = numPages;
    }

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
     * Gets the author.
     * 
     * @return the author
     */
    public String getAuthor()
    {
        return author;
    }

    /**
     * Sets the author.
     * 
     * @param author
     *            the new author
     */
    public void setAuthor(String author)
    {
        this.author = author;
    }

    /**
     * Gets the category.
     * 
     * @return the category
     */
    public String getCategory()
    {
        return category;
    }

    /**
     * Sets the category.
     * 
     * @param category
     *            the new category
     */
    public void setCategory(String category)
    {
        this.category = category;
    }

    /**
     * Gets the num pages.
     * 
     * @return the num pages
     */
    public int getNumPages()
    {
        return numPages;
    }

    /**
     * Sets the num pages.
     * 
     * @param number
     *            the new num pages
     */
    public void setNumPages(int number)
    {
        this.numPages = number;
    }

}
