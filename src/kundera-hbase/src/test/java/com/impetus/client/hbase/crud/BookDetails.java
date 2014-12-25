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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author Pragalbh Garg
 *
 */
@Entity
@Table(name = "book_details", schema = "KunderaExamples@hbaseTest")
public class BookDetails
{
    @Id
    @Column(name = "book_details")
    @GeneratedValue(strategy = GenerationType.TABLE)
    private String id;

    /** The isbn. */
    private String isbn;

    /** The copies_num. */
    @Column(name = "copies_num")
    private int copiesNum;

    public int getCopiesNum()
    {
        return copiesNum;
    }

    public void setCopiesNum(int copiesNum)
    {
        this.copiesNum = copiesNum;
    }

    /** The hits. */
    private int hits;

    public int getHits()
    {
        return hits;
    }

    public void setHits(int hits)
    {
        this.hits = hits;
    }

    public String getIsbn()
    {
        return isbn;
    }

    public void setIsbn(String isbn)
    {
        this.isbn = isbn;
    }
}
