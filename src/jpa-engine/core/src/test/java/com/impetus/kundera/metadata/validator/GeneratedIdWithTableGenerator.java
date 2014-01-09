package com.impetus.kundera.metadata.validator;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.TableGenerator;

@Entity
@Table(name = "GeneratedIdWithTableGenerator", schema = "KunderaTest@GeneratedValue")
public class GeneratedIdWithTableGenerator
{

    @Id
    @TableGenerator(name = "id_gen", allocationSize = 30, initialValue = 100, schema = "KunderaTest", table = "kundera", pkColumnName = "sequence", valueColumnName = "sequenceValue", pkColumnValue = "kk")
    @GeneratedValue(generator = "id_gen", strategy = GenerationType.TABLE)
    private int id;

    @Column
    private String name;

    /**
     * @return the id
     */
    public int getId()
    {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(int id)
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
