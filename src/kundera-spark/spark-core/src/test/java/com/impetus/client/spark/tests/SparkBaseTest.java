package com.impetus.client.spark.tests;

import java.util.List;

import junit.framework.Assert;

import com.impetus.client.spark.entities.Person;


/**
 * The Class SparkBaseTest.
 */
public abstract class SparkBaseTest
{

 
    /**
     * Gets the person.
     *
     * @param id the id
     * @param name the name
     * @param age the age
     * @param salary the salary
     * @return the person
     */
    public Person getPerson(String id, String name, Integer age, Double salary)
    {
        Person person = new Person();
        person.setAge(age);
        person.setPersonId(id);
        person.setPersonName(name);
        person.setSalary(salary);
        return person;
    }
    
    /**
     * Validate person1.
     *
     * @param person the person
     */
    public void validatePerson1(Person person)
    {
        Assert.assertNotNull(person);
        Assert.assertEquals("dev", person.getPersonName());
        Assert.assertEquals(22, person.getAge());
        Assert.assertEquals(30000.5, person.getSalary());
    }

    /**
     * Validate person2.
     *
     * @param person the person
     */
    public void validatePerson2(Person person)
    {
        Assert.assertNotNull(person);
        Assert.assertEquals("pg", person.getPersonName());
        Assert.assertEquals(23, person.getAge());
        Assert.assertEquals(40000.6, person.getSalary());
    }

    /**
     * Validate person3.
     *
     * @param person the person
     */
    public void validatePerson3(Person person)
    {
        Assert.assertNotNull(person);
        Assert.assertEquals("kpm", person.getPersonName());
        Assert.assertEquals(24, person.getAge());
        Assert.assertEquals(50000.7, person.getSalary());
    }

    /**
     * Assert results.
     *
     * @param results the results
     * @param foundPerson1 the found person1
     * @param foundPerson2 the found person2
     * @param foundPerson3 the found person3
     */
    public void assertResults(List<Person> results, boolean foundPerson1, boolean foundPerson2,
            boolean foundPerson3)
    {
        for (Person person : results)
        {
            switch (person.getPersonId())
            {
            case "1":
                if (foundPerson1)
                    validatePerson1(person);
                else
                    Assert.fail();
                break;
            case "2":
                if (foundPerson2)
                    validatePerson2(person);
                else
                    Assert.fail();
                break;
            case "3":
                if (foundPerson3)
                    validatePerson3(person);
                else
                    Assert.fail();
                break;
            }
        }
    }

    
    
}
