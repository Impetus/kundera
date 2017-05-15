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
 * The Class ArticleMongo.
 */
@Entity
@Table(name = "Article", schema = "KunderaExamples@mongoTest")
public class ArticleMongo
{

    @Id
    @Column(name = "article_id")
    private String articleId;

    @Column(name = "create_date", nullable = false)
    private Date createDate;

    @Column(name = "display_date")
    private Date displayDate;

    @Column(name = "title", nullable = false)
    private String title;

    @Column(name = "category")
    private String category;

    @Column(name = "priority")
    private int priority;

    @Column(name = "show")
    private boolean show;

    public String getArticleId()
    {
        return articleId;
    }

    public void setArticleId(final String articleId)
    {
        this.articleId = articleId;
    }

    public Date getCreateDate()
    {
        return createDate;
    }

    public void setCreateDate(final Date createDate)
    {
        this.createDate = createDate;
    }

    public Date getDisplayDate()
    {
        return displayDate;
    }

    public void setDisplayDate(final Date displayDate)
    {
        this.displayDate = displayDate;
    }

    public String getTitle()
    {
        return title;
    }

    public void setTitle(final String title)
    {
        this.title = title;
    }

    public String getCategory()
    {
        return category;
    }

    public void setCategory(final String category)
    {
        this.category = category;
    }

    public int getPriority()
    {
        return priority;
    }

    public void setPriority(final int priority)
    {
        this.priority = priority;
    }

    public boolean isShow()
    {
        return show;
    }

    public void setShow(final boolean show)
    {
        this.show = show;
    }
}
