package com.impetus.kundera.metadata.entities;

import javax.persistence.Column;

/**
 * @author vivek.mishra
 * 
 */

public class RootMappedSuperClass
{

    @Column
    private String rootStr;

    @Column
    private Long rootLong;

    /**
     * @return the rootStr
     */
    public String getRootStr()
    {
        return rootStr;
    }

    /**
     * @param rootStr
     *            the rootStr to set
     */
    public void setRootStr(String rootStr)
    {
        this.rootStr = rootStr;
    }

    /**
     * @return the rootLong
     */
    public Long getRootLong()
    {
        return rootLong;
    }

    /**
     * @param rootLong
     *            the rootLong to set
     */
    public void setRootLong(Long rootLong)
    {
        this.rootLong = rootLong;
    }

}
