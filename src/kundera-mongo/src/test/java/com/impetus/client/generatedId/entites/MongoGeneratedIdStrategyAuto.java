package com.impetus.client.generatedId.entites;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.TableGenerator;

import org.bson.types.ObjectId;

@Entity
@Table(name = "MongoGeneratedIdStrategyAuto", schema = "KunderaExamples@mongoTest")
@TableGenerator(name = "id_gen")
public class MongoGeneratedIdStrategyAuto
{
    @Id
    @GeneratedValue(generator = "id_gen", strategy = GenerationType.AUTO)
    private ObjectId id;

    @Column
    private String name;

    /**
     * @return the id
     */
    public ObjectId getId()
    {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(ObjectId id)
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
