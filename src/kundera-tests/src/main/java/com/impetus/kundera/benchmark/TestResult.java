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
package com.impetus.kundera.benchmark;

/**
 * Keeps the metadata of test result.
 * 
 * @author Kuldeep Mishra
 * 
 */
public class TestResult
{
    private Double totalTimeTaken;

    private Integer noOfInvocation;

    private Double avgTime;

    private Double delta;

    /**
     * 
     */
    public TestResult()
    {

    }

    /**
     * 
     */
    public TestResult(Double totalTimeTaken, Integer noOfInvocation, Double avgTime)
    {
        this.totalTimeTaken = totalTimeTaken;
        this.noOfInvocation = noOfInvocation;
        this.avgTime = avgTime;
    }

    /**
     * @return the totalTimeTaken
     */
    public Double getTotalTimeTaken()
    {
        return totalTimeTaken;
    }

    /**
     * @param totalTimeTaken
     *            the totalTimeTaken to set
     */
    public void setTotalTimeTaken(Double totalTimeTaken)
    {
        this.totalTimeTaken = totalTimeTaken;
    }

    /**
     * @return the noOfInvocation
     */
    public Integer getNoOfInvocation()
    {
        return noOfInvocation;
    }

    /**
     * @param noOfInvocation
     *            the noOfInvocation to set
     */
    public void setNoOfInvocation(Integer noOfInvocation)
    {
        this.noOfInvocation = noOfInvocation;
    }

    /**
     * @return the avgTime
     */
    public Double getAvgTime()
    {
        return avgTime;
    }

    /**
     * @param avgTime
     *            the avgTime to set
     */
    public void setAvgTime(Double avgTime)
    {
        this.avgTime = avgTime;
    }

    /**
     * @return the delta
     */
    public Double getDelta()
    {
        return delta;
    }

    /**
     * @param delta
     *            the delta to set
     */
    public void setDelta(Double delta)
    {
        this.delta = delta;
    }

}
