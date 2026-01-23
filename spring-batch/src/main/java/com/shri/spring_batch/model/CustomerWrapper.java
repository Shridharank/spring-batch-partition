package com.shri.spring_batch.model;

import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CustomerWrapper {
    private JpaCustomer jpaCustomer;
    private ElasticCustomer elasticCustomer;
}
