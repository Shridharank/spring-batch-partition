package com.shri.spring_batch.service;

import org.springframework.batch.infrastructure.item.file.separator.SimpleRecordSeparatorPolicy;

public class RangeRecordSeparatorPolicy extends SimpleRecordSeparatorPolicy {

    private final int maxLine;
    private int count;
    public RangeRecordSeparatorPolicy(int maxLine) {
        this.maxLine = maxLine;
    }

    @Override
    public String postProcess(String record) {
        if(++count > maxLine) {
            return null;
        }
        return record;
    }
}
