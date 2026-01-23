package com.shri.spring_batch.config;

import com.shri.spring_batch.exception.CustomerValidationException;
import com.shri.spring_batch.listener.CustomerErrorSkipListener;
import com.shri.spring_batch.listener.ErrorWriterStepListener;
import com.shri.spring_batch.model.CustomerInput;
import com.shri.spring_batch.model.CustomerWrapper;
import com.shri.spring_batch.model.JpaCustomer;
import com.shri.spring_batch.processor.CustomerItemProcessor;
import com.shri.spring_batch.repository.CustomerJpaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.listener.SkipListener;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.infrastructure.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static com.shri.spring_batch.util.CustomerBatchUtil.*;

@Configuration
@RequiredArgsConstructor
public class JpaRepoBatchConfig {

    private final JobRepository jobRepository;
    private final CustomerJpaRepository jpaRepository;

    //Reader
    @Bean
    @StepScope
    public FlatFileItemReader<CustomerInput> jpaItemReader() {

        return new FlatFileItemReaderBuilder<CustomerInput>()
                .name("jpaCsvReader")
                .resource(new FileSystemResource("src/main/resources/dummy_users_1000_with_accounts.csv"))
                .linesToSkip(1)
                .lineMapper(readLineMapper())
                .build();
    }

    //Processor
    @Bean
    public CustomerItemProcessor jpaItemProcessor() {
        return new CustomerItemProcessor();
    }

    //Writer
    @Bean
    public ItemWriter<CustomerWrapper> jpaItemWriter() {
        return items -> {
            List<JpaCustomer> list = items.getItems().stream()
                    .map(CustomerWrapper::getJpaCustomer).toList();
            jpaRepository.saveAll(list);
        };
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<CustomerWrapper> processedCustomerWriter(
            @Value("${batch.output-file}") String outputDir,
            @Value("#{stepExecutionContext['partitionName']}") String partitionName
    ) throws IOException {
        Path dir = Paths.get(outputDir, "processed");
        Files.createDirectories(dir);

        return new FlatFileItemWriterBuilder<CustomerWrapper>()
                .name("processedCustomerWriter")
                .headerCallback(writer -> writer.write("id,firstName,lastName,email, gender," +
                        "contactNo,country,dob,fromAccountNumber,toAccountNumber, amount"))
                .resource(new FileSystemResource(
                        dir.resolve("jpaProcessedRecord+" + partitionName + "+.csv")))
                .shouldDeleteIfEmpty(true)
                .lineAggregator(processedLineAggregator("jpaCustomer"))
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<CustomerInput> errorCustomerWriterCsv(
            @Value("${batch.output-file}") String outputDir,
            @Value("#{stepExecutionContext['partitionName']}") String partitionName) throws IOException {
        Path dir = Paths.get(outputDir, "error");
        Files.createDirectories(dir);

        return new FlatFileItemWriterBuilder<CustomerInput>()
                .name("errorCustomerWriterCsv")
                .headerCallback(writer -> writer.write("id,firstName,lastName,email, gender," +
                        "contactNo,country,dob,fromAccountNumber,toAccountNumber, amount"))
                .resource(new FileSystemResource(
                        dir.resolve("esErrorRecord+" + partitionName + "+.csv")))
                .shouldDeleteIfEmpty(true)
                .lineAggregator(errorLineAggregator())
                .build();

    }

    @Bean
    public CompositeItemWriter<CustomerWrapper> compositeJpaItemWriter(
            ItemWriter<CustomerWrapper> jpaItemWriter,
            FlatFileItemWriter<CustomerWrapper> processedCustomerWriter
    ) {

        CompositeItemWriter<CustomerWrapper> compositeItemWriter = new CompositeItemWriter<>();
        compositeItemWriter.setDelegates(List.of(jpaItemWriter, processedCustomerWriter));
        return compositeItemWriter;
    }

    @Bean
    public Step jpaRepoStep(SkipListener<CustomerInput, CustomerWrapper> customerErrorSkipListener,
                            CompositeItemWriter <CustomerWrapper> compositeJpaItemWriter,
                            StepExecutionListener errorWriterStepListener) {
        return new StepBuilder("csvImport", jobRepository)
                .<CustomerInput, CustomerWrapper>chunk(100)
                .reader(jpaItemReader())
                .processor(jpaItemProcessor())
                .writer(compositeJpaItemWriter)
                .faultTolerant()
                .skip(CustomerValidationException.class)
                .skipLimit(Integer.MAX_VALUE)
                .skipListener(customerErrorSkipListener)
                .listener(errorWriterStepListener)
                .taskExecutor(jpaTaskExecutor())
                .build();
    }

    @Bean
    public Job jpaRepoJob(Step jpaRepoStep) {
        return new JobBuilder("customerJpaJob", jobRepository)
                .start(jpaRepoStep)
                .build();
    }

    @Bean
    public AsyncTaskExecutor jpaTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(5);
        taskExecutor.setMaxPoolSize(10);
        taskExecutor.setQueueCapacity(25);
        taskExecutor.setThreadNamePrefix("jpa-batch-");
        taskExecutor.initialize();
        return taskExecutor;
    }

    @Bean
    public SkipListener<CustomerInput, CustomerWrapper> customerErrorSkipListener(
            FlatFileItemWriter<CustomerInput> errorCustomerWriterCsv) {
        return new CustomerErrorSkipListener(errorCustomerWriterCsv);
    }

    @Bean
    public StepExecutionListener errorWriterStepListener(
            FlatFileItemWriter<CustomerInput> errorCustomerWriterCsv) {
        return new ErrorWriterStepListener(errorCustomerWriterCsv);
    }

    private String getDateTimeFormat() {
        LocalDateTime now = LocalDateTime.now();
        return DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(now);
    }
}
