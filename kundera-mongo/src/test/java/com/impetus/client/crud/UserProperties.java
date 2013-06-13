package com.impetus.client.crud;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@Embeddable
public class UserProperties
{
    @Column
    private String hi;

    @Column
    private List<String> tagList;

    @Column
    private Map<String, String> propertyKeyMap;

    @Column
    private Set<String> propertyValueSet;

    public UserProperties()
    {
        tagList = new LinkedList<String>();
        propertyKeyMap = new HashMap<String, String>();
        propertyValueSet = new HashSet<String>();
        tagList.add("hurry");
        propertyKeyMap.put("xamry", "Amresh");
        propertyValueSet.add("hi");
        hi = "hello";
    }

    public String getHi()
    {
        return hi;
    }

    public List<String> getTags()
    {
        return tagList;
    }

    public Map<String, String> getPropertyKeys()
    {
        return propertyKeyMap;
    }

    public Set<String> getPropertyValues()
    {
        return propertyValueSet;
    }
}
