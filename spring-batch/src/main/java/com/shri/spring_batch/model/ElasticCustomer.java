package com.shri.spring_batch.model;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

import java.math.BigDecimal;

@Builder
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Document(indexName = "customers")
public class ElasticCustomer {

    @Id
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
