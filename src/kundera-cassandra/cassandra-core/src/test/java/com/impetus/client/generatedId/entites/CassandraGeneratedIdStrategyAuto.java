package com.impetus.client.generatedId.entites;

import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.TableGenerator;

@Entity
@Table(name = "CassandraGeneratedIdStrategyAuto", schema = "kunderaGeneratedId@cassandra_generated_id")
@TableGenerator(name = "id_gen")
public class CassandraGeneratedIdStrategyAuto
{
    @Id
    @GeneratedValue(generator = "id_gen", strategy = GenerationType.AUTO)
    private UUID id;

    @Column
    private String name;

    /**
     * @return the id
     */
    public UUID getId()
    {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(UUID id)
    {
        this.id = id;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name)
    {
        this.name = name;
    }

}
