package com.schwab.batch.processor;

import com.schwab.batch.model.Person;
import org.springframework.batch.item.ItemProcessor;

public class PersonItemProcessor implements ItemProcessor<Person, Person> {
    @Override
    public Person process(Person person) throws Exception {
        System.out.println("Processing: " + person);
        if (person.getFirstName() != null) {
            final String initCapFirstName = person.getFirstName().substring(0, 1).toUpperCase()
                    + person.getFirstName().substring(1);
            final String initCapLastName = person.getLastName().substring(0, 1).toUpperCase()
                    + person.getLastName().substring(1);
            Person transformedPerson = new Person();
            transformedPerson.setId(person.getId());
            transformedPerson.setFirstName(initCapFirstName);
            transformedPerson.setLastName(initCapLastName);
            transformedPerson.setAddress(person.getAddress());
            return transformedPerson;
        } else
            return person;
    }
}
