package com.shri.spring_batch.model;

import lombok.*;

import java.math.BigDecimal;

@Getter
@Setter
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CustomerInput {
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
