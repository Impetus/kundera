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
package com.impetus.client.es;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class Book.
 * 
 * @author devender.yadav
 * 
 */
@Entity
@Table(name = "BOOK", schema = "esSchema@es-refresh-indexes-pu")
public class Book
{

    /** The book id. */
    @Id
    @Column(name = "BOOK_ID")
    private int bookId;

    /** The title. */
    @Column(name = "TITLE")
    private String title;

    /** The author. */
    @Column(name = "AUTHOR")
    private String author;

    /** The num pages. */
    @Column(name = "NUM_PAGES")
    private int numPages;

    /**
     * Gets the book id.
     *
     * @return the book id
     */
    public int getBookId()
    {
        return bookId;
    }

    /**
     * Sets the book id.
     *
     * @param bookId
     *            the new book id
     */
    public void setBookId(int bookId)
    {
        this.bookId = bookId;
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
     * @param numPages
     *            the new num pages
     */
    public void setNumPages(int numPages)
    {
        this.numPages = numPages;
    }

}
