package com.impetus.kundera.persistence.context.jointable;

import junit.framework.Assert;

import org.junit.Test;

import com.impetus.kundera.persistence.context.jointable.JoinTableData.OPERATION;
import com.impetus.kundera.polyglot.entities.PersonBMM;

public class JoinTableMetadataTest
{

    @Test
    public void test()
    {
        JoinTableData joinTableData = new JoinTableData(OPERATION.INSERT,"test",null,null,null,null);
        Assert.assertNotNull(joinTableData);
        Assert.assertEquals(OPERATION.INSERT, joinTableData.getOperation());
        Assert.assertNull(joinTableData.getJoinColumnName());
        joinTableData.setJoinColumnName("testjoinColumn");
        Assert.assertNotNull(joinTableData.getJoinColumnName());
        Assert.assertNull(joinTableData.getInverseJoinColumnName());
        joinTableData.setInverseJoinColumnName("testInverseJoinColumn");
        Assert.assertNotNull(joinTableData.getInverseJoinColumnName());
        Assert.assertNull(joinTableData.getJoinTableName());
        joinTableData.setJoinTableName("testJoinTable");
        Assert.assertNotNull(joinTableData.getJoinTableName());
        joinTableData.setOperation(OPERATION.DELETE);
        
       Assert.assertNull(joinTableData.getEntityClass());
       joinTableData.setEntityClass(PersonBMM.class);
       Assert.assertNotNull(joinTableData.getEntityClass());
    }

}
