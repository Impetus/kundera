/**
 * Copyright 2012 Impetus Infotech.
 *
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
package com.impetus.kundera.rest.common;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedNativeQueries;
import javax.persistence.NamedNativeQuery;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Entity class for Book
 * 
 * @author amresh.singh
 */

@Entity
@Table(name = "BOOK", schema = "KunderaExamples@twissandra")
@NamedQueries(value = { @NamedQuery(name = "findByAuthor", query = "Select b from Book b where b.author = :author"),
        @NamedQuery(name = "findByPublication", query = "Select b from Book b where b.publication = ?1"),
        @NamedQuery(name = "findAllBooks", query = "Select b from Book b") })
@NamedNativeQueries(value = { @NamedNativeQuery(name = "findAllBooksNative", query = "select * from " + "BOOK") })
@XmlRootElement
public class Book
{
    @Id
    @Column(name = "ISBN")
    String isbn;

    @Column(name = "AUTHOR")
    String author;

    @Column(name = "PUBLICATION")
    String publication;

    /**
     * @return the isbn
     */
    public String getIsbn()
    {
        return isbn;
    }

    /**
     * @param isbn
     *            the isbn to set
     */
    public void setIsbn(String isbn)
    {
        this.isbn = isbn;
    }

    /**
     * @return the author
     */
    public String getAuthor()
    {
        return author;
    }

    /**
     * @param author
     *            the author to set
     */
    public void setAuthor(String author)
    {
        this.author = author;
    }

    /**
     * @return the publication
     */
    public String getPublication()
    {
        return publication;
    }

    /**
     * @param publication
     *            the publication to set
     */
    public void setPublication(String publication)
    {
        this.publication = publication;
    }

}
