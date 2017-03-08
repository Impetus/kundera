package com.impetus.client.crud.entities;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Embeddable;

@Embeddable
public class CompositeId {

   @Basic
   @Column(name = "first_name")
   private String firstName;

   @Basic
   @Column(name = "birth_date")
   private String birthDate;

   public String getFirstName()
   {
      return firstName;
   }

   public void setFirstName(final String firstName)
   {
      this.firstName = firstName;
   }

   public String getBirthDate()
   {
      return birthDate;
   }

   public void setBirthDate(final String birthDate)
   {
      this.birthDate = birthDate;
   }
}
