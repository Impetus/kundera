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
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;

/**
 * The Class ArticleMTO.
 */
@Entity
@Table(name = "ArticleMTO", schema = "KunderaExamples@mongoTest")
public class ArticleMTO
{

   @Id
   @Column(name = "ext_id")
   private String id;

   @ManyToOne
   @JoinColumn(name = "article_id")
   private ArticleMongo article;

   @OneToOne
   @JoinColumn(name = "details_id")
   private ArticleDetails details;

   @Column(name = "article_value")
   private long value;

   public String getId()
   {
      return id;
   }

   public void setId(final String id)
   {
      this.id = id;
   }

   public ArticleMongo getArticle()
   {
      return article;
   }

   public void setArticle(final ArticleMongo article)
   {
      this.article = article;
   }

   public ArticleDetails getDetails()
   {
      return details;
   }

   public void setDetails(final ArticleDetails details)
   {
      this.details = details;
   }

   public long getValue()
   {
      return value;
   }

   public void setValue(final long value)
   {
      this.value = value;
   }
}
