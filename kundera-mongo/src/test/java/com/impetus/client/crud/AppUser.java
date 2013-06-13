package com.impetus.client.crud;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "User", schema = "KunderaExamples@mongoTest")
public class AppUser
{
    @Id
    private String id;

    @Column
    private List<String> tags;

    @Column
    private Map<String, String> propertyKeys;

    @Column
    private Set<String> propertyValues;

    @Column
    protected List<String> searchList;

    @Embedded
    private UserProperties propertyContainer;

    public AppUser()
    {
        tags = new LinkedList<String>();
        propertyKeys = new HashMap<String, String>();
        propertyValues = new HashSet<String>();
        searchList = new LinkedList<String>();
        tags.add("yo");
        propertyKeys.put("kk", "Kuldeep");
        propertyValues.add("hey");
        tags.add("yo");
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public List<String> getTags()
    {
        return tags;
    }

    public Map<String, String> getPropertyKeys()
    {
        return propertyKeys;
    }

    public Set<String> getPropertyValues()
    {
        return propertyValues;
    }

    public List<String> getSearchList()
    {
        return searchList;
    }

    public UserProperties getPropertyContainer()
    {
        return propertyContainer;
    }

    public void setPropertyContainer(UserProperties propertyContainer)
    {
        this.propertyContainer = propertyContainer;
    }
}
