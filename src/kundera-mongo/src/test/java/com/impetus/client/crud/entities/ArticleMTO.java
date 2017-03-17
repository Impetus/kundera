package com.impetus.client.crud.entities;

import javax.persistence.*;

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
