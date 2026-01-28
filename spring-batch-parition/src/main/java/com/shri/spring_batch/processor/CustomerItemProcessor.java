package com.shri.spring_batch.processor;

import com.shri.spring_batch.exception.CustomerValidationException;
import com.shri.spring_batch.model.CustomerInput;
import com.shri.spring_batch.model.CustomerWrapper;
import com.shri.spring_batch.model.ElasticCustomer;
import com.shri.spring_batch.model.JpaCustomer;
import org.springframework.batch.infrastructure.item.ItemProcessor;

import java.util.Objects;
import java.util.regex.Pattern;

public class CustomerItemProcessor implements ItemProcessor<CustomerInput, CustomerWrapper> {

    private final Pattern ALPHA_PATTERN = Pattern.compile("^[a-zA-Z]+$");
    private static final Pattern EMAIL_PATTERN = Pattern.compile("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$");

    @Override
    public CustomerWrapper process(CustomerInput customerInput) {
        validate(customerInput);
        ElasticCustomer elasticCustomer = mapToElasticCustomer(customerInput);
        JpaCustomer jpaCustomer = mapToJpaCustomer(customerInput);

        return CustomerWrapper.builder()
                .elasticCustomer(elasticCustomer)
                .jpaCustomer(jpaCustomer)
                .build();
    }

    private void validate(CustomerInput customerInput) {
        if(Objects.isNull(customerInput)) {
            throw new CustomerValidationException("Customer input is null");
        }

        if(!isAlphaString(customerInput.getFirstName())) {
            throw new CustomerValidationException("Invalid first name: " + customerInput.getFirstName());
        }

        if(!isAlphaString(customerInput.getLastName())) {
            throw new CustomerValidationException("Invalid last name: " + customerInput.getLastName());
        }
        if(!isValidEmail(customerInput.getEmail())) {
            throw new CustomerValidationException("Invalid email: " + customerInput.getEmail());
        }

        if(!isValidGender(customerInput.getGender())) {
            throw new CustomerValidationException("Invalid gender: " + customerInput.getGender());
        }

        /*if(!isValidPhoneNumber(customerInput.getContactNo())) {
            throw new CustomerValidationException("Invalid contact number: " + customerInput.getContactNo());
        }*/

        if(!isValidDate(customerInput.getDob())) {
            throw new CustomerValidationException("Invalid date of birth: " + customerInput.getDob());
        }
    }

    private boolean isAlphaString(String s) {
        return s != null && ALPHA_PATTERN.matcher(s.trim()).matches();
    }

    private boolean isValidEmail(String email) {
        return email != null && EMAIL_PATTERN.matcher(email.trim()).matches();
    }

    private boolean isValidGender(String gender) {
        if(gender == null)
            return false;

        String g = gender.trim().toLowerCase();
        return "male".equals(g) || "female".equals(g) || "other".equals(g);
    }

    private boolean isValidPhoneNumber(String contactNo) {
        // Simple validation: check if it's not null and has 10 digits
        return contactNo != null && contactNo.matches("\\d{10}");
    }

    private boolean isValidDate(String dob) {
        // Simple validation: check if it's not null and matches YYYY-MM-DD format
        return dob != null && dob.matches("\\d{4}-\\d{2}-\\d{2}");
    }

    private ElasticCustomer mapToElasticCustomer(CustomerInput customerInput) {
        ElasticCustomer elasticCustomer = new ElasticCustomer();
        elasticCustomer.setId(customerInput.getId());
        elasticCustomer.setFirstName(customerInput.getFirstName().toUpperCase());
        elasticCustomer.setLastName(customerInput.getLastName().toUpperCase());
        elasticCustomer.setGender(customerInput.getGender());
        elasticCustomer.setEmail(customerInput.getEmail().toLowerCase());
        elasticCustomer.setContactNo(customerInput.getContactNo());
        elasticCustomer.setDob(customerInput.getDob());
        elasticCustomer.setCountry(customerInput.getCountry());
        elasticCustomer.setFromAccountNumber(customerInput.getFromAccountNumber());
        elasticCustomer.setToAccountNumber(customerInput.getToAccountNumber());
        elasticCustomer.setAmount(customerInput.getAmount());
        return elasticCustomer;
    }

    private JpaCustomer mapToJpaCustomer(CustomerInput customerInput) {
        JpaCustomer jpaCustomer = new JpaCustomer();
        jpaCustomer.setId(null);
        jpaCustomer.setFirstName(customerInput.getFirstName().toUpperCase());
        jpaCustomer.setLastName(customerInput.getLastName().toUpperCase());
        jpaCustomer.setGender(customerInput.getGender());
        jpaCustomer.setEmail(customerInput.getEmail().toLowerCase());
        jpaCustomer.setContactNo(customerInput.getContactNo());
        jpaCustomer.setDob(customerInput.getDob());
        jpaCustomer.setCountry(customerInput.getCountry());
        jpaCustomer.setFromAccountNumber(customerInput.getFromAccountNumber());
        jpaCustomer.setToAccountNumber(customerInput.getToAccountNumber());
        jpaCustomer.setAmount(customerInput.getAmount());
        return jpaCustomer;
    }
}
