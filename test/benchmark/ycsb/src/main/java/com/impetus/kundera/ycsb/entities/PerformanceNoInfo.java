/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.ycsb.entities;

import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

/**
 * @author Kuldeep Mishra
 * 
 */
@Entity
@Table(name = "PerformanceNo")
public class PerformanceNoInfo
{
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id")
    private int id;

    @Column
    @Temporal(TemporalType.DATE)
    private Date date;

    @Column
    private String clientType;

    @Column
    private double releaseNo;

    @Column
    private String testType;

    @Column
    private int noOfThreads;

    @Column
    private long noOfOperations;

    @Column
    private double totalTimeTaken;

    @Column
    private int runSequence;

    @Column
    private BigDecimal avgLatency;
    
    @Column BigDecimal throughput;
    
    
    /**
     * @return the date
     */
    public Date getDate()
    {
        return date;
    }

    /**
     * @param date
     *            the date to set
     */
    public void setDate(Date date)
    {
        this.date = date;
    }

    /**
     * @return the clientType
     */
    public String getClientType()
    {
        return clientType;
    }

    /**
     * @param clientType
     *            the clientType to set
     */
    public void setClientType(String clientType)
    {
        this.clientType = clientType;
    }

    /**
     * 
     */
    public PerformanceNoInfo()
    {

    }

    /**
     * 
     */
    public PerformanceNoInfo(Date date, double releaseNo, String clientType, String testType, int noOfThreads,
            long noOfOperations, double totalTimeTaken, int runSequence)
    {
        this.date = date;
        this.clientType = clientType;
        this.runSequence = runSequence;
        this.releaseNo = releaseNo;
        this.testType = testType;
        this.noOfThreads = noOfThreads;
        this.noOfOperations = noOfOperations;
        this.totalTimeTaken = totalTimeTaken;
    }

    /**
     * @return the releaseNo
     */
    public double getReleaseNo()
    {
        return releaseNo;
    }

    /**
     * @param releaseNo
     *            the releaseNo to set
     */
    public void setReleaseNo(double releaseNo)
    {
        this.releaseNo = releaseNo;
    }

    /**
     * @return the testType
     */
    public String getTestType()
    {
        return testType;
    }

    /**
     * @param testType
     *            the testType to set
     */
    public void setTestType(String testType)
    {
        this.testType = testType;
    }

    /**
     * @return the noOfThreads
     */
    public int getNoOfThreads()
    {
        return noOfThreads;
    }

    /**
     * @param noOfThreads
     *            the noOfThreads to set
     */
    public void setNoOfThreads(int noOfThreads)
    {
        this.noOfThreads = noOfThreads;
    }

    /**
     * @return the noOfOperations
     */
    public long getNoOfOperations()
    {
        return noOfOperations;
    }

    /**
     * @param noOfOperations
     *            the noOfOperations to set
     */
    public void setNoOfOperations(long noOfOperations)
    {
        this.noOfOperations = noOfOperations;
    }

    /**
     * @return the totalTimeTaken
     */
    public double getTotalTimeTaken()
    {
        return totalTimeTaken;
    }

    /**
     * @param totalTimeTaken
     *            the totalTimeTaken to set
     */
    public void setTotalTimeTaken(double totalTimeTaken)
    {
        this.totalTimeTaken = totalTimeTaken;
    }

    /**
     * @return the runSequence
     */
    public int getRunSequence()
    {
        return runSequence;
    }

    /**
     * @param runSequence
     *            the runSequence to set
     */
    public void setRunSequence(int runSequence)
    {
        this.runSequence = runSequence;
    }

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

    public BigDecimal getAvgLatency()
    {
        return avgLatency;
    }

    public void setAvgLatency(BigDecimal avgLatency)
    {
        this.avgLatency = avgLatency;
    }

    public BigDecimal getThroughput()
    {
        return throughput;
    }

    public void setThroughput(BigDecimal throughput)
    {
        this.throughput = throughput;
    }

    
}
