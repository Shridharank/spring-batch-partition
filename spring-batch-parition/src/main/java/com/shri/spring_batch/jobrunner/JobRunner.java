package com.shri.spring_batch.jobrunner;

public interface JobRunner {
    void run(String inputFilePath) throws Exception;
}
