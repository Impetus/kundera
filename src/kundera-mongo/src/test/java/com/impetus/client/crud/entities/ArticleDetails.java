/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.crud.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

/**
 * The Class ArticleDetails.
 */
@Entity
@Table(name = "ArticleDetails", schema = "KunderaExamples@mongoTest")
public class ArticleDetails
{

    @Id
    @Column(name = "details_id")
    private String id;

    @Column(name = "author")
    private String author;

    @Column(name = "intro")
    private String intro;

    @Column(name = "body")
    private String body;

    public String getId()
    {
        return id;
    }

    public void setId(final String id)
    {
        this.id = id;
    }

    public String getAuthor()
    {
        return author;
    }

    public void setAuthor(final String author)
    {
        this.author = author;
    }

    public String getIntro()
    {
        return intro;
    }

    public void setIntro(final String intro)
    {
        this.intro = intro;
    }

    public String getBody()
    {
        return body;
    }

    public void setBody(final String body)
    {
        this.body = body;
    }
}
