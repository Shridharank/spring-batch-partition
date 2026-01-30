package com.shri.kafka.model.model;

import lombok.*;

@Builder
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ElasticCustomer {

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
