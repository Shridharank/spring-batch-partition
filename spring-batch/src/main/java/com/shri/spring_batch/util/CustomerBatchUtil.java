package com.shri.spring_batch.util;

import com.shri.spring_batch.model.CustomerInput;
import com.shri.spring_batch.model.CustomerWrapper;
import org.springframework.batch.infrastructure.item.file.LineMapper;
import org.springframework.batch.infrastructure.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.infrastructure.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.infrastructure.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.infrastructure.item.file.transform.LineAggregator;

public class CustomerBatchUtil {

    public static LineMapper<CustomerInput> readLineMapper() {
        return new DefaultLineMapper<>() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames("id", "firstName", "lastName", "email", "gender",
                                "contactNo", "country", "dob", "fromAccountNumber", "toAccountNumber", "amount");
                    }
                });
                setFieldSetMapper(new BeanWrapperFieldSetMapper<>() {
                    {
                        setTargetType(CustomerInput.class);
                    }
                });
            }
        };
    }

    public static LineAggregator<CustomerWrapper> processedLineAggregator(String type) {
        return new DelimitedLineAggregator<>() {
            {
                setDelimiter(",");
                setFieldExtractor(new BeanWrapperFieldExtractor<>() {
                    {
                        setNames(new String[]{
                                type + ".id", type + ".firstName", type + ".lastName", type + ".email", type + ".gender",
                                type + ".contactNo", type + ".country", type + ".dob", type + ".fromAccountNumber",
                                type + ".toAccountNumber", type + ".amount"
                        });
                    }
                });
            }
        };
    }

    public static LineAggregator<CustomerInput> errorLineAggregator() {
        return new DelimitedLineAggregator<>() {
            {
                setDelimiter(",");
                setFieldExtractor(new BeanWrapperFieldExtractor<>() {
                    {
                        setNames(new String[]{
                                "id", "firstName", "lastName", "email", "gender",
                                "contactNo", "country", "dob", "fromAccountNumber", "toAccountNumber", "amount"
                        });
                    }
                });
            }
        };
    }
}
