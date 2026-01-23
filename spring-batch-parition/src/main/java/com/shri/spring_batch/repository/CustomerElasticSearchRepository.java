package com.shri.spring_batch.repository;

import com.shri.spring_batch.model.ElasticCustomer;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface CustomerElasticSearchRepository extends ElasticsearchRepository<ElasticCustomer, Integer> {

}
