package com.shri.spring_batch.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.*;

import java.math.BigDecimal;

@Builder
@Entity
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class JpaCustomer {

    @Id
    @GeneratedValue
    private Integer id;

    private String firstName;
    private String lastName;
    private String email;
    private String gender;
    private String contactNo;
    private String country;
    private String dob;
    private String fromAccountNumber;
    private String toAccountNumber;
    private String amount;

}
