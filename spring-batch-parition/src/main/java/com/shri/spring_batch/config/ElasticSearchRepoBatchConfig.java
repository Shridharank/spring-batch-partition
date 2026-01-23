package com.shri.spring_batch.config;

import com.shri.spring_batch.exception.CustomerValidationException;
import com.shri.spring_batch.listener.CustomerErrorSkipListener;
import com.shri.spring_batch.listener.ErrorWriterStepListener;
import com.shri.spring_batch.model.CustomerInput;
import com.shri.spring_batch.model.CustomerWrapper;
import com.shri.spring_batch.model.ElasticCustomer;
import com.shri.spring_batch.processor.CustomerItemProcessor;
import com.shri.spring_batch.repository.CustomerElasticSearchRepository;
import com.shri.spring_batch.service.FlatFileItemPartitioner;
import com.shri.spring_batch.service.RangeRecordSeparatorPolicy;
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
public class ElasticSearchRepoBatchConfig {

    private final CustomerElasticSearchRepository elasticsearchRepository;
    private final JobRepository jobRepository;
    private final FlatFileItemPartitioner flatFileItemPartitioner;

    @Bean
    @StepScope
    public FlatFileItemReader<CustomerInput> esPartitionedItemReaderCsv(
            @Value("#{stepExecutionContext[startLine]}") Integer start,
            @Value("#{stepExecutionContext[endLine]}") Integer end
    ) {

        System.out.println("Partitioned Item Reader Start Line: " + start + ", End Line: " + end);
        return new FlatFileItemReaderBuilder<CustomerInput>()
                .name("esPartitionedItemReaderCsv")
                .resource(new FileSystemResource("src/main/resources/dummy_users_1000_with_accounts.csv"))
                .linesToSkip(1)
                .recordSeparatorPolicy(new RangeRecordSeparatorPolicy(end - start + 1))
                .lineMapper(readLineMapper())
                .build();
    }

    @Bean
    public CustomerItemProcessor esItemProcessor() {
        return new CustomerItemProcessor();
    }

    @Bean
    @StepScope
    public ItemWriter<CustomerWrapper> itemWriterElasticRepo() {
        return items -> {
            List<ElasticCustomer> list = items.getItems().stream()
                    .map(CustomerWrapper::getElasticCustomer).toList();
            elasticsearchRepository.saveAll(list);
        };
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<CustomerWrapper> processedCustomerWriterCsv(
            @Value("${batch.output-file}") String outputDir,
            @Value("#{stepExecutionContext['partitionName']}") String partitionName
    ) throws IOException {
        System.out.println("Processed Writer Partition Name: " + partitionName);
        Path dir = Paths.get(outputDir, "processed");
        Files.createDirectories(dir);

        return new FlatFileItemWriterBuilder<CustomerWrapper>()
                .name("processedCustomerWriterCsv")
                .headerCallback(writer -> writer.write("id,firstName,lastName,email, gender," +
                        "contactNo,country,dob,fromAccountNumber,toAccountNumber, amount"))
                .resource(new FileSystemResource(
                        dir.resolve("esProcessedRecord+" + partitionName + "+.csv")))
                .append(true)
                .shouldDeleteIfEmpty(true)
                .lineAggregator(processedLineAggregator("elasticCustomer"))
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
                .lineAggregator(errorLineAggregator())
                .headerCallback(writer -> writer.write("id,firstName,lastName,email, gender," +
                        "contactNo,country,dob,fromAccountNumber,toAccountNumber, amount"))
                .resource(new FileSystemResource(dir.resolve("esErrorRecord+" + partitionName + "+.csv")))
                .shouldDeleteIfEmpty(true)
                .build();
    }

    @Bean
    public CompositeItemWriter<CustomerWrapper> compositeItemWriter(
            ItemWriter<CustomerWrapper> itemWriterElasticRepo,
            FlatFileItemWriter<CustomerWrapper> processedCustomerWriterCsv
    ) {
        CompositeItemWriter<CustomerWrapper> compositeItemWriter = new CompositeItemWriter<>();
        compositeItemWriter.setDelegates(List.of(itemWriterElasticRepo, processedCustomerWriterCsv));
        return compositeItemWriter;
    }

    @Bean
    public Step elasticSearchSlaveStep(FlatFileItemReader<CustomerInput> esPartitionedItemReaderCsv,
                                       CompositeItemWriter<CustomerWrapper> compositeItemWriter,
                                       SkipListener<CustomerInput, CustomerWrapper> customerErrorSkipListener,
                                       StepExecutionListener errorWriterStepListener) {
        return new StepBuilder("slaveStep", jobRepository)
                .<CustomerInput, CustomerWrapper>chunk(100)
                .reader(esPartitionedItemReaderCsv)
                .processor(esItemProcessor())
                .writer(compositeItemWriter)
                .faultTolerant()
                .skip(CustomerValidationException.class)
                .skipListener(customerErrorSkipListener)
                .listener(errorWriterStepListener)
                .build();
    }

    @Bean
    public Step masterStep(Step elasticSearchSlaveStep) {
        return new StepBuilder("masterStep", jobRepository)
                .partitioner("slaveStep", flatFileItemPartitioner)
                .step(elasticSearchSlaveStep)
                .gridSize(4)
                .taskExecutor(esTaskExecutor())
                .build();
    }

    @Bean
    public Job elasticSearchRepoJob(Step masterStep) {
        return new JobBuilder("customerElasticJob", jobRepository)
                .start(masterStep)
                .build();
    }

    @Bean
    public AsyncTaskExecutor esTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setMaxPoolSize(10);
        taskExecutor.setThreadNamePrefix("elastic-batch-");
        taskExecutor.initialize();
        return taskExecutor;
    }

    @Bean
    public SkipListener<CustomerInput, CustomerWrapper> customerErrorSkipListener(
            FlatFileItemWriter<CustomerInput> errorCustomerWriter) {
        return new CustomerErrorSkipListener(errorCustomerWriter);
    }

    @Bean
    public StepExecutionListener errorWriterStepListener(
            FlatFileItemWriter<CustomerInput> errorCustomerWriter) {
        return new ErrorWriterStepListener(errorCustomerWriter);
    }

    private String getDateTimeFormat() {
        LocalDateTime now = LocalDateTime.now();
        return DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").format(now);
    }
}
