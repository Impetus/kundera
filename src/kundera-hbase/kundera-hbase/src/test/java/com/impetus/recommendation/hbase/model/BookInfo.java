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
package com.impetus.recommendation.hbase.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 
 * @author Kuldeep.Mishra
 *
 */
@Entity
@Table(name = "bookinfo", schema = "KunderaExamples@hbaseTest")
public class BookInfo
{
    @Id
    @Column(name = "bookId")
    private String bookId;

    @Column(name = "md5")
    private String md5;

    @Column(name = "title")
    private String title;

    @Column(name = "author")
    private String author;

    @Column(name = "yearofpub")
    private String yearofpub;

    @Column(name = "publisher")
    private String publisher;

    @Column(name = "imageurls")
    private String imageurls;

    @Column(name = "imageurlm")
    private String imageurlm;

    @Column(name = "imageurll")
    private String imageurll;

    @Column(name = "price")
    private Float price;

    public String getBookId()
    {
        return bookId;
    }

    public void setBookId(String bookId)
    {
        this.bookId = bookId;
    }

    public String getMd5()
    {
        return md5;
    }

    public void setMd5(String md5)
    {
        this.md5 = md5;
    }

    public String getTitle()
    {
        return title;
    }

    public void setTitle(String title)
    {
        this.title = title;
    }

    public String getAuthor()
    {
        return author;
    }

    public void setAuthor(String author)
    {
        this.author = author;
    }

    public String getYearofpub()
    {
        return yearofpub;
    }

    public void setYearofpub(String yearofpub)
    {
        this.yearofpub = yearofpub;
    }

    public String getPublisher()
    {
        return publisher;
    }

    public void setPublisher(String publisher)
    {
        this.publisher = publisher;
    }

    public String getImageurls()
    {
        return imageurls;
    }

    public void setImageurls(String imageurls)
    {
        this.imageurls = imageurls;
    }

    public String getImageurlm()
    {
        return imageurlm;
    }

    public void setImageurlm(String imageurlm)
    {
        this.imageurlm = imageurlm;
    }

    public String getImageurll()
    {
        return imageurll;
    }

    public void setImageurll(String imageurll)
    {
        this.imageurll = imageurll;
    }

    public Float getPrice()
    {
        return price;
    }

    public void setPrice(Float price)
    {
        this.price = price;
    }
}
