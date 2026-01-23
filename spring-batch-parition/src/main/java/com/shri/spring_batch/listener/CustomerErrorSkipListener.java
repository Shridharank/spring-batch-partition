package com.shri.spring_batch.listener;

import com.shri.spring_batch.exception.CustomerValidationException;
import com.shri.spring_batch.model.CustomerInput;
import com.shri.spring_batch.model.CustomerWrapper;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.listener.SkipListener;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;

@RequiredArgsConstructor
@StepScope
public class CustomerErrorSkipListener implements SkipListener<CustomerInput, CustomerWrapper> {

    private final FlatFileItemWriter<CustomerInput> errorCustomerWriter;

    @Override
    public void onSkipInProcess(CustomerInput item, Throwable t) {
        System.out.println("On skip process item: " + item);
        if (t instanceof CustomerValidationException) {
            try {
                System.out.println("Skipping item: " + item);

                Chunk<CustomerInput> chunk = new Chunk<>();
                chunk.add(item);

                errorCustomerWriter.write(chunk);
            } catch (Exception e) {
                throw new RuntimeException("Error writing error record", e);
            }
        }
    }

    @Override
    public void onSkipInRead(Throwable t) {
        System.out.println("Skipped in READ: " + t.getMessage());
    }

    @Override
    public void onSkipInWrite(CustomerWrapper item, Throwable t) {
        System.out.println("Skipped in WRITE: " + item);
    }
}
