package com.shri.spring_batch.config;

import com.shri.spring_batch.exception.CustomerValidationException;
import com.shri.spring_batch.listener.CustomerErrorSkipListener;
import com.shri.spring_batch.listener.ErrorWriterStepListener;
import com.shri.spring_batch.model.CustomerInput;
import com.shri.spring_batch.model.CustomerWrapper;
import com.shri.spring_batch.model.JpaCustomer;
import com.shri.spring_batch.processor.CustomerItemProcessor;
import com.shri.spring_batch.repository.CustomerJpaRepository;
import com.shri.spring_batch.service.FlatFileItemPartitioner;
import jakarta.persistence.EntityManagerFactory;
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
import org.springframework.batch.infrastructure.item.database.JpaItemWriter;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.infrastructure.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
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
    private final FlatFileItemPartitioner flatFileItemPartitioner;

    //Reader
    @Bean
    @StepScope
    public FlatFileItemReader<CustomerInput> jpaItemReaderCsv(
            @Value("#{jobParameters['inputFilePath']}") String inputFilePath
    ) throws IOException {
        Resource resource = new DefaultResourceLoader().getResource(inputFilePath);
        System.out.println("Resource file path: "+resource.getFilePath());
        return new FlatFileItemReaderBuilder<CustomerInput>()
                .name("jpaItemReaderCsv")
                .resource(resource)
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
    @StepScope
    public ItemWriter<CustomerWrapper> jpaItemWriter(EntityManagerFactory emf) {
        System.out.println("Jpa Repo write");
        /*JpaItemWriter<CustomerWrapper> jpaItemWriter = new JpaItemWriter<>(emf);
        return jpaItemWriter;*/
        return items -> {
            List<JpaCustomer> list = items.getItems().stream()
                    .map(CustomerWrapper::getJpaCustomer).toList();
            System.out.println("List to save: "+list);
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
    @StepScope
    public CompositeItemWriter<CustomerWrapper> compositeJpaItemWriter(
            ItemWriter<CustomerWrapper> jpaItemWriter,
            FlatFileItemWriter<CustomerWrapper> processedCustomerWriter
    ) {

        CompositeItemWriter<CustomerWrapper> compositeItemWriter = new CompositeItemWriter<>();
        compositeItemWriter.setDelegates(List.of(jpaItemWriter, processedCustomerWriter));
        return compositeItemWriter;
    }

    @Bean
    public Step jpaSlaveStep(FlatFileItemReader<CustomerInput> jpaItemReaderCsv,
                            SkipListener<CustomerInput, CustomerWrapper> customerErrorSkipListener,
                            CompositeItemWriter <CustomerWrapper> compositeJpaItemWriter,
                            StepExecutionListener errorWriterStepListener) {
        return new StepBuilder("csvImport", jobRepository)
                .<CustomerInput, CustomerWrapper>chunk(100)
                .reader(jpaItemReaderCsv)
                .processor(jpaItemProcessor())
                .writer(compositeJpaItemWriter)
                .faultTolerant()
                .skip(CustomerValidationException.class)
                .skipLimit(Integer.MAX_VALUE)
                .skipListener(customerErrorSkipListener)
                .listener(errorWriterStepListener)
                .build();
    }

    public Step masterStep(Step jpaSlaveStep) {
        return new StepBuilder("masterStep", jobRepository)
                .partitioner("jpaSlaveStep", flatFileItemPartitioner)
                .step(jpaSlaveStep)
                .gridSize(4)
                .taskExecutor(jpaTaskExecutor())
                .build();
    }

    @Bean
    public Job jpaRepoJob(Step masterStep) {
        return new JobBuilder("jpaRepoJob", jobRepository)
                .start(masterStep)
                .build();
    }

    @Bean
    public AsyncTaskExecutor jpaTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setMaxPoolSize(10);
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
