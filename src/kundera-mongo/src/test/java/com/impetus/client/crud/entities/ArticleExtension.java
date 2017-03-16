package com.impetus.client.crud.entities;

import javax.persistence.*;

@Entity
@Table(name = "ArticleExtension", schema = "KunderaExamples@mongoTest")
public class ArticleExtension
{

   @Id
   @Column(name = "ext_id")
   private String id;

   @ManyToOne
   @JoinColumn(name = "article_id")
   private ArticleMongo article;

   @Column(name = "artile_value")
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

   public long getValue()
   {
      return value;
   }

   public void setValue(final long value)
   {
      this.value = value;
   }
}
