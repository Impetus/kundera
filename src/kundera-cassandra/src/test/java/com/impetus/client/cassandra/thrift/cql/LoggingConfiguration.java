package com.impetus.client.cassandra.thrift.cql;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Entity
@Table(name = "LoggingConfiguration", schema = "KunderaExamples@twissandraTest")
@IndexCollection(columns = { @Index(name = "logname") })
public class LoggingConfiguration
{
    @Id
    private String id;

    @Column
    private String label;

    @Column
    private String logname;

    @Column
    private Timestamp nnow;

    @Column
    private String lvl;

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public String getLable()
    {
        return label;
    }

    public void setLable(String lable)
    {
        this.label = lable;
    }

    public String getLongName()
    {
        return logname;
    }

    public void setLongName(String longName)
    {
        this.logname = longName;
    }

    public String getLvl()
    {
        return lvl;
    }

    public void setLvl(String lvl)
    {
        this.lvl = lvl;
    }

    public Timestamp getNnow()
    {
        return nnow;
    }

    public void setNnow(Timestamp nnow)
    {
        this.nnow = nnow;
    }

}
