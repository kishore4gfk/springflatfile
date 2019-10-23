package com.schwab.batch.mapper;

import com.schwab.batch.model.Address;
import com.schwab.batch.model.Person;
import com.schwab.batch.model.Submitter;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;

public class PersonFieldSetMapper implements FieldSetMapper<Person> {
    @Override
    public Person mapFieldSet(FieldSet fieldSet) {
        Person person = new Person();
        String recordType = fieldSet.readString(0);
        if (recordType.equalsIgnoreCase("RE")) {
            person.setId(fieldSet.readInt(1));
            person.setFirstName(fieldSet.readString(2));
            person.setLastName(fieldSet.readString(3));
            Address address = new Address();
            address.setLine1(fieldSet.readString(4));
            address.setLine2(fieldSet.readString(5));
            address.setLine3(fieldSet.readString(6));
            person.setAddress(address);
        } else {
            Submitter submitter = new Submitter();
            submitter.setId(fieldSet.readInt(1));
            submitter.setName(fieldSet.readString(2));
            person.setSubmitter(submitter);
        }
        return person;
    }
}
