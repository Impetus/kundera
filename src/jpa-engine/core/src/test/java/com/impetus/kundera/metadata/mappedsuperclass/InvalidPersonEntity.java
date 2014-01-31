package com.impetus.kundera.metadata.mappedsuperclass;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;

//TODOO: While doing for attribute override, i need to verify with hibernate if i am having id attribute

@MappedSuperclass
@Entity
public class InvalidPersonEntity
{

    @Id
    private String id;

    @Column
    private String firstName;

    @Column
    private String lastName;

    // try with @Id attribute on Employee as well.
    // @Column
    private transient int version;

    public String getFirstName()
    {
        return firstName;
    }

    public void setFirstName(String firstName)
    {
        this.firstName = firstName;
    }

    public String getLastName()
    {
        return lastName;
    }

    public void setLastName(String lastName)
    {
        this.lastName = lastName;
    }

    public int getVersion()
    {
        return version;
    }

    public void setVersion(int version)
    {
        this.version = version;
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

}
