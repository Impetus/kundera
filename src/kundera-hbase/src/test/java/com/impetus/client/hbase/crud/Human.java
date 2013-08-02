package com.impetus.client.hbase.crud;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

@Entity
@Table(name = "Humans", schema = "KunderaExamples@ilpMainSchema")
public class Human
{
    @Id
    public String hId;

    @Column(name = "humanAlive")
    public Boolean humanAlive;

    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "humanId")
    // @Column(name = "HumansPrivatePhoto")
    public HumansPrivatePhoto humansPrivatePhoto;

    public Human()
    {

    }

    public Human(String humanId)
    {
        this.hId = humanId;
    }

    public Boolean getHumanAlive()
    {
        return humanAlive;
    }

    public void setHumanAlive(Boolean humanAlive)
    {
        this.humanAlive = humanAlive;
    }

    public HumansPrivatePhoto getHumansPrivatePhoto()
    {
        return humansPrivatePhoto;
    }

    public void setHumansPrivatePhoto(HumansPrivatePhoto humansPrivatePhoto)
    {
        this.humansPrivatePhoto = humansPrivatePhoto;
    }

    public String getHumanId()
    {
        return hId;
    }

}
