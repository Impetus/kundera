package com.impetus.client.crud;

import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Entity
@Table(name = "groups", schema = "KunderaExamples@secIdxCassandraTest")
@IndexCollection(columns = { @Index(name = "parentId") })
public class Group
{

    @Id
    @Column(name = "resourceId")
    private String resourceId;

    @ManyToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @JoinColumn(name = "parentId")
    private Group parent;

    @OneToMany(fetch = FetchType.EAGER, mappedBy = "parent")
    private List<Group> children;

    @Column(name = "resourceName")
    private String resourceName;

    public Group()
    {
    }

    public Group getParent()
    {
        return parent;
    }

    public void setParent(Group parent)
    {
        this.parent = parent;
    }

    public List<Group> getChildren()
    {
        return children;
    }

    public void setChildren(List<Group> children)
    {
        this.children = children;
    }

    public String getResourceId()
    {
        return resourceId;
    }

    public void setResourceId(String resourceId)
    {
        this.resourceId = resourceId;
    }

    public String getResourceName()
    {
        return resourceName;
    }

    public void setResourceName(String resourceName)
    {
        this.resourceName = resourceName;
    }

}
