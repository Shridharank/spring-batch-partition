package com.shri.spring_batch.jobrunner.impl;

import com.shri.spring_batch.jobrunner.JobRunner;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component("jpaJobRunner")
public class JpaJobRunner implements JobRunner {

    private final JobOperator jobOperator;
    private final Job jpaRepoJob;

    public JpaJobRunner(JobOperator jobOperator,
                        @Qualifier("jpaRepoJob") Job jpaRepoJob) {
        this.jpaRepoJob = jpaRepoJob;
        this.jobOperator = jobOperator;
    }
    @Override
    public void run(String inputFilePath) throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("startAt",System.currentTimeMillis())
                .addString("inputFilePath", inputFilePath)
                .toJobParameters();
        jobOperator.start(jpaRepoJob, jobParameters);
    }
}
