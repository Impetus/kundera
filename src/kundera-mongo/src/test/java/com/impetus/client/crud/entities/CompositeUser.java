package com.impetus.client.crud.entities;

import javax.persistence.*;

@Entity
@Table(name = "composite_user")
public class CompositeUser {

   @EmbeddedId
   private CompositeId id;

   @Basic
   @Column(name = "phone")
   private String phone;

   public CompositeId getId()
   {
      return id;
   }

   public void setId(final CompositeId id)
   {
      this.id = id;
   }

   public String getPhone()
   {
      return phone;
   }

   public void setPhone(final String phone)
   {
      this.phone = phone;
   }

}
