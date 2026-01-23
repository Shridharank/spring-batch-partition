package com.shri.spring_batch.listener;

import com.shri.spring_batch.model.CustomerInput;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;

@RequiredArgsConstructor
public class ErrorWriterStepListener  implements StepExecutionListener {

    private final FlatFileItemWriter<CustomerInput> errorCustomerWriter;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        errorCustomerWriter.open(stepExecution.getExecutionContext());
    }

    @Override
    public @Nullable ExitStatus afterStep(StepExecution stepExecution) {

        errorCustomerWriter.close();
        System.out.println("status:"+stepExecution.getStatus());
        return stepExecution.getExitStatus();
    }
}
