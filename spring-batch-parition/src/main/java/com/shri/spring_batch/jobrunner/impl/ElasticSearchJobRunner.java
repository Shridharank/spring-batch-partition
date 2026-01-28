package com.shri.spring_batch.jobrunner.impl;

import com.shri.spring_batch.jobrunner.JobRunner;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameter;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.observability.jfr.events.job.JobLaunchEvent;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component("elasticSearchJobRunner")
public class ElasticSearchJobRunner implements JobRunner {

    private final JobOperator jobOperator;
    private final Job elasticSearchRepoJob;


    public ElasticSearchJobRunner (JobOperator jobOperator,
                                   @Qualifier("elasticSearchRepoJob") Job elasticSearchRepoJob) {
        this.elasticSearchRepoJob = elasticSearchRepoJob;
        this.jobOperator = jobOperator;
    }
    @Override
    public void run(String inputFilePath) throws Exception {
        System.out.println("Job Runner file path :"+inputFilePath);
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("startAt",System.currentTimeMillis())
                .addString("inputFilePath", inputFilePath)
                .toJobParameters();

        jobOperator.start(elasticSearchRepoJob, jobParameters);
    }
}
