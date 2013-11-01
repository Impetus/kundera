package com.impetus.client.crud;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Table(name = "PERSONNEL", schema = "KunderaExamples@mongoTest")
@Entity
public class PersonnelEmbedded
{
    @Id
    @Column
    private int id;

    @Column
    private String name;

    @Column
    private int age;

    @Embedded
    private PersonalDetailEmbedded personalDetail;

    public int getId()
    {
        return id;
    }

    public void setId(int id)
    {
        this.id = id;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public int getAge()
    {
        return age;
    }

    public void setAge(int age)
    {
        this.age = age;
    }

    public PersonalDetailEmbedded getPersonalDetail()
    {
        return personalDetail;
    }

    public void setPersonalDetail(PersonalDetailEmbedded personalDetail)
    {
        this.personalDetail = personalDetail;
    }
}