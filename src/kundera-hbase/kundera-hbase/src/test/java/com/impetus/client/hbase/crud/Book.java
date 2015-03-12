/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
package com.impetus.client.hbase.crud;

import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * @author Pragalbh Garg
 *
 */
@Entity
@Table(name = "books", schema = "KunderaExamples@hbaseTest")
@IndexCollection(columns = { @Index(name = "bookId"), @Index(name = "title"), @Index(name = "author"),
        @Index(name = "description"), @Index(name = "category"), @Index(name = "language"), @Index(name="bookDetails") })
public class Book
{

    /** The book_id. */
    @Id
    @Column(name = "book_id")
    @GeneratedValue(strategy = GenerationType.TABLE)
    private String bookId;

    /** The title. */
    private String title;

    /** The author. */
    private String author;

    /** The description. */
    private String description;

    /** The publisher. */
    private String publisher;

    /** The image. */
    private String image;

    /** The language. */
    @ManyToOne(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinColumn(name = "language")
    private Languages language;

    /** The category. */
    @ManyToMany(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinTable(name = "book_category", schema = "KunderaExamples", joinColumns = { @JoinColumn(name = "book_id") }, inverseJoinColumns = { @JoinColumn(name = "categories_id") })
    private Set<Categories> category;

    @OneToOne(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinColumn(name = "book_details")
    private BookDetails bookDetails;

    public BookDetails getBookDetails()
    {
        return bookDetails;
    }

    public void setBookDetails(BookDetails bookDetails)
    {
        this.bookDetails = bookDetails;
    }

    public Book()
    {

    }

    public Book(String bookId2, String title2)
    {
        setBookId(bookId2);
        setTitle(title2);
    }

    public void setBookId(String bookId)
    {
        this.bookId = bookId;
    }

    /**
     * Gets the category.
     * 
     * @return the category
     */
    public Set<Categories> getCategory()
    {
        return category;
    }

    /**
     * Sets the category.
     * 
     * @param category
     *            the new category
     */
    public void setCategory(Set<Categories> category)
    {
        this.category = category;
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
     * Gets the book_id.
     * 
     * @return the book_id
     */
    public String getBookId()
    {
        return bookId;
    }

    /**
     * Gets the description.
     * 
     * @return the description
     */
    public String getDescription()
    {
        return description;
    }

    /**
     * Sets the description.
     * 
     * @param description
     *            the new description
     */
    public void setDescription(String description)
    {
        this.description = description;
    }

    /**
     * Gets the publisher.
     * 
     * @return the publisher
     */
    public String getPublisher()
    {
        return publisher;
    }

    /**
     * Sets the publisher.
     * 
     * @param publisher
     *            the new publisher
     */
    public void setPublisher(String publisher)
    {
        this.publisher = publisher;
    }

    /**
     * Gets the image.
     * 
     * @return the image
     */
    public String getImage()
    {
        return image;
    }

    /**
     * Sets the image.
     * 
     * @param image
     *            the new image
     */
    public void setImage(String image)
    {
        this.image = image;
    }

    /**
     * Gets the language.
     * 
     * @return the language
     */
    public Languages getLanguage()
    {
        return language;
    }

    /**
     * Sets the language.
     * 
     * @param language
     *            the new language
     */
    public void setLanguage(Languages language)
    {
        this.language = language;
    }

}
