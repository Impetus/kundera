package com.impetus.client.hbase.crud;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.Table;

@Entity
@Table(name = "HumansPrivatePhoto", schema = "KunderaExamples@ilpMainSchema")
public class HumansPrivatePhoto
{

    @Id
    public String humanId;


    @OneToOne(mappedBy = "humansPrivatePhoto", cascade = CascadeType.REFRESH)
    //@PrimaryKeyJoinColumn
    public Human human;
    
    public String photoName;
    
    public HumansPrivatePhoto()
    {
        
    }
    
    public HumansPrivatePhoto(String humanId)
    {
        this.humanId = humanId;
    }
    public Human getHuman()
    {
        return human;
    }

    public void setHuman(Human human)
    {
        this.human = human;
    }

    public String getPhotoName()
    {
        return photoName;
    }

    public void setPhotoName(String photoName)
    {
        this.photoName = photoName;
    }

    public String getHumanId()
    {
        return humanId;
    }
    
    
}
