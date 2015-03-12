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

import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * @author Pragalbh Garg
 *
 */
@Entity
@Table(name = "book_languages", schema = "KunderaExamples@hbaseTest")
@IndexCollection(columns = { @Index(name = "languageId"), @Index(name = "language") })
public class Languages
{

    /** The language_id. */
    @Id
    @GeneratedValue(strategy = GenerationType.TABLE)
    @Column(name = "language_id")
    private String languageId;

    @OneToMany(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER, mappedBy = "language")
    private List<Book> books;
    public List<Book> getBooks()
    {
        return books;
    }

    public void setBooks(List<Book> books)
    {
        this.books = books;
    }

    /** The language. */
    private String language;

    /**
     * Gets the language.
     * 
     * @return the language
     */
    public String getLanguage()
    {
        return language;
    }

    /**
     * Sets the language.
     * 
     * @param language
     *            the new language
     */
    public void setLanguage(String language)
    {
        this.language = language;
    }

    /**
     * Gets the language_id.
     * 
     * @return the language_id
     */
    public String getLanguageId()
    {
        return languageId;
    }

    /**
     * Sets the language_id.
     * 
     * @param languageId
     *            the new language_id
     */
    public void setLanguageId(String languageId)
    {
        this.languageId = languageId;
    }
}
