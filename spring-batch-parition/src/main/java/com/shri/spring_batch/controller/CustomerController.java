package com.shri.spring_batch.controller;

import com.shri.spring_batch.jobrunner.JobRunner;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/customers")
@RequiredArgsConstructor
public class CustomerController {

    //private final JobOperator jobOperator;
    private final Map<String, JobRunner> jobRunnerMap;

    @Value("${batch.input-file}")
    private String inputFilePath;

    /*@Qualifier("elasticSearchRepoJob")
    private final Job elasticSearchRepoJob;
    @Qualifier("jpaRepoJob")
    private final Job jpaRepoJob;

    @PostMapping("/jpa")
    public void jpaImportCsvToDbJob() {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("startAt",System.currentTimeMillis())
                        .toJobParameters();
        try {
            jobOperator.start(jpaRepoJob, jobParameters);
        } catch (JobInstanceAlreadyCompleteException | JobExecutionAlreadyRunningException |
                 InvalidJobParametersException | JobRestartException e) {
            e.printStackTrace();
        }
    }

    @PostMapping("/elastic-search")
    public void esImportCsvToDbJob() {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("startAt",System.currentTimeMillis())
                .toJobParameters();
        try {
            jobOperator.start(elasticSearchRepoJob, jobParameters);
        } catch (JobInstanceAlreadyCompleteException | JobExecutionAlreadyRunningException |
                 InvalidJobParametersException | JobRestartException e) {
            e.printStackTrace();
        }
    }*/

    @GetMapping("/run-job/{type}")
    public String runJob(@PathVariable String type) throws Exception {
        JobRunner jobRunner = jobRunnerMap.get(type + "JobRunner");
        if(jobRunner == null) {
            return "No job found for type: " + type;
        }

        jobRunner.run(inputFilePath);
        return type.toLowerCase()+" import job started.";
    }
}
