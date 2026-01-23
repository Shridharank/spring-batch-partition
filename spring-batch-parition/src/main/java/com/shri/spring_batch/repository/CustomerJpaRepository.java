package com.shri.spring_batch.repository;

import com.shri.spring_batch.model.JpaCustomer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CustomerJpaRepository extends JpaRepository<JpaCustomer, Integer> {
}
