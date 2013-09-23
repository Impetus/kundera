package com.impetus.client.crud;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Entity
@Table(name = "tests", schema = "KunderaTests@cassandra_pu")
@IndexCollection(columns = { @Index(name = "likedBy"), @Index(name = "income"), @Index(name = "settlementDate"),
        @Index(name = "dateSet"), @Index(name = "and"), @Index(name = "between"), @Index(name = "or") })
public class EntityWithClause
{
    @Id
    private String id;

    @Column
    private String likedBy;

    @Column
    private String income;

    @Column
    private String settlementDate;

    @Column
    private String dateSet;

    @Column
    private String and;

    @Column
    private String between;

    @Column
    private String or;

    @Column
    private String set;

    // @ElementCollection
    // @CollectionTable
    // private List<CurvePoint> curvePoints;

    public String getLikedBy()
    {
        return likedBy;
    }

    public void setLikedBy(String likedBy)
    {
        this.likedBy = likedBy;
    }

    public String getIncome()
    {
        return income;
    }

    public void setIncome(String income)
    {
        this.income = income;
    }

    public String getSettlementDate()
    {
        return settlementDate;
    }

    public void setSettlementDate(String settlementDate)
    {
        this.settlementDate = settlementDate;
    }

    public String getAnd()
    {
        return and;
    }

    public void setAnd(String and)
    {
        this.and = and;
    }

    public String getBetween()
    {
        return between;
    }

    public void setBetween(String between)
    {
        this.between = between;
    }

    public String getDateSet()
    {
        return dateSet;
    }

    public void setDateSet(String dateSet)
    {
        this.dateSet = dateSet;
    }

    public EntityWithClause()
    {
    }

    // public List<CurvePoint> getCurvePoints()
    // {
    // return curvePoints;
    // }
    //
    // public void setCurvePoints(List<CurvePoint> curvePoints)
    // {
    // this.curvePoints = curvePoints;
    // }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public EntityWithClause(String id/* , List<CurvePoint> curvePoints */)
    {
        this.id = id;
        // this.curvePoints = curvePoints;
    }

    public String getOr()
    {
        return or;
    }

    public void setOr(String or)
    {
        this.or = or;
    }

    public String getSet()
    {
        return set;
    }

    public void setSet(String set)
    {
        this.set = set;
    }

}