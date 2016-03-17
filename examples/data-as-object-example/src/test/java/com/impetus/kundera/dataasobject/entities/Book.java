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
import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * The Class Book.
 * 
 */
@Entity
@IndexCollection(columns = { @Index(name = "title"), @Index(name = "author"), @Index(name = "year"),
        @Index(name = "pages") })
public class Book extends DefaultKunderaEntity<Book, Integer>
{
    /** The book_id. */
    @Id
    @Column(name = "ID")
    private Integer bookId;

    /** The title. */
    @Column(name = "TITLE")
    private String title;

    /** The author. */
    @Column(name = "AUTHOR")
    private String author;

    /** The year. */
    @Column(name = "YEAR")
    private int year;

    /** The pages. */
    @Column(name = "PAGES")
    private int pages;

    /**
     * Gets the book id.
     * 
     * @return the book id
     */
    public Integer getBookId()
    {
        return bookId;
    }

    /**
     * Sets the book id.
     * 
     * @param bookId
     *            the new book id
     */
    public void setBookId(Integer bookId)
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
     * Gets the year.
     * 
     * @return the year
     */
    public int getYear()
    {
        return year;
    }

    /**
     * Sets the year.
     * 
     * @param year
     *            the new year
     */
    public void setYear(int year)
    {
        this.year = year;
    }

    /**
     * Gets the pages.
     * 
     * @return the pages
     */
    public int getPages()
    {
        return pages;
    }

    /**
     * Sets the pages.
     * 
     * @param pages
     *            the new pages
     */
    public void setPages(int pages)
    {
        this.pages = pages;
    }

}
