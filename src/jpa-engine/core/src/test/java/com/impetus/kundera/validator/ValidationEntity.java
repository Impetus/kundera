/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.validator;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.validation.constraints.AssertFalse;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Digits;
import javax.validation.constraints.Future;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import javax.validation.constraints.Past;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

/**
 * @author Chhavi Gangwal
 *
 */
@Entity
public class ValidationEntity
{
    
    @Id
    private String  eId;
    
    @Column
    @NotNull(message="Age should not be null")
    private int age;
    
    @Column
    @Null(message="The value should be null.")
    private String nullField;
    
    @Column
    @AssertFalse(message="The  person type must be human")
    private boolean isHuman;
    
    @Column
    @AssertTrue(message="The person type must be I-human")
    private boolean isIHuman;
    
    @Column
    @DecimalMax(message="Invalid Decimal max value.", value = "20")
    private int decimalMax;
    
        
    @Column
    @DecimalMin(message="Invalid Decimal min value.", value = "50")
    private int decimalMin;
    
      
    @Column
    @Digits(message="Invalid number.", fraction = 0, integer = 0)
    private int digits;
    
    @Column
    @Future(message="Invalid future date.")
    private java.util.Date future;
    
    @Column
    @Past(message="Invalid past date")
    private java.util.Date past;
    
    @Column
    @Pattern(message="Invalid pattern.", regexp = "^.+@.+\\.")
    private String pattern;
    
    @Column
    @Size(message="Invalid size.", max= 50, min=10)
    private String size;
    
    @Column
    @Max(message="Invalid max value.", value = 100)
    private String max;
    
    @Column
    @Min(message="Invalid min value.", value = -20)
    private int min;
    
    @NotNull
    @Size(min = 1, max = 256)
    @Pattern(regexp="^.+@.+\\..+$")
    @Column(name="email")
    private String email;
   

    /**
     * @param age the age to set
     */
    public void setAge(int age)
    {
        this.age = age;
    }

    /**
     * @return the age
     */
    public int getAge()
    {
        return age;
    }

    /**
     * @param eId the eId to set
     */
    public void seteId(String eId)
    {
        this.eId = eId;
    }

    /**
     * @return the eId
     */
    public String geteId()
    {
        return eId;
    }

    /**
     * @param isHuman the isHuman to set
     */
    public void setHuman(boolean isHuman)
    {
        this.isHuman = isHuman;
    }

    /**
     * @return the isHuman
     */
    public boolean isHuman()
    {
        return isHuman;
    }

    /**
     * @param nullField the nullField to set
     */
    public void setNullField(String nullField)
    {
        this.nullField = nullField;
    }

    /**
     * @return the nullField
     */
    public String getNullField()
    {
        return nullField;
    }

    /**
     * @param isIHuman the isIHuman to set
     */
    public void setIHuman(boolean isIHuman)
    {
        this.isIHuman = isIHuman;
    }

    /**
     * @return the isIHuman
     */
    public boolean isIHuman()
    {
        return isIHuman;
    }

    /**
     * @param decimalMax the decimalMax to set
     */
    public void setDecimalMax(int decimalMax)
    {
        this.decimalMax = decimalMax;
    }

    /**
     * @return the decimalMax
     */
    public int getDecimalMax()
    {
        return decimalMax;
    }

    /**
     * @param decimalMin the decimalMin to set
     */
    public void setDecimalMin(int decimalMin)
    {
        this.decimalMin = decimalMin;
    }

    /**
     * @return the decimalMin
     */
    public int getDecimalMin()
    {
        return decimalMin;
    }

    /**
     * @param digits the digits to set
     */
    public void setDigits(int digits)
    {
        this.digits = digits;
    }

    /**
     * @return the digits
     */
    public int getDigits()
    {
        return digits;
    }

    /**
     * @param future the future to set
     */
    public void setFuture(java.util.Date future)
    {
        this.future = future;
    }

    /**
     * @return the future
     */
    public java.util.Date getFuture()
    {
        return future;
    }

    /**
     * @param pastDate the past to set
     */
    public void setPast(java.util.Date pastDate)
    {
        this.past = pastDate;
    }

    /**
     * @return the past
     */
    public java.util.Date getPast()
    {
        return past;
    }

    /**
     * @param pattern the pattern to set
     */
    public void setPattern(String pattern)
    {
        this.pattern = pattern;
    }

    /**
     * @return the pattern
     */
    public String getPattern()
    {
        return pattern;
    }

    /**
     * @param size the size to set
     */
    public void setSize(String  size)
    {
        this.size = size;
    }

    /**
     * @return the size
     */
    public String getSize()
    {
        return size;
    }

    /**
     * @param max the max to set
     */
    public void setMax(String max)
    {
        this.max = max;
    }

    /**
     * @return the max
     */
    public String getMax()
    {
        return max;
    }

    /**
     * @param min the min to set
     */
    public void setMin(int min)
    {
        this.min = min;
    }

    /**
     * @return the min
     */
    public int getMin()
    {
        return min;
    }

    /**
     * @param email the email to set
     */
    public void setEmail(String email)
    {
        this.email = email;
    }

    /**
     * @return the email
     */
    public String getEmail()
    {
        return email;
    }

}
