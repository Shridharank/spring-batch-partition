package com.shri.spring_batch.service;

import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class FlatFileItemPartitioner implements Partitioner {
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {

        Map<String, ExecutionContext> partitionMap = new HashMap<>();

        int totalLines = 1000; // Assume total lines in the file
        int linesPerPartition = totalLines / gridSize;

        int start = 1;
        int end = linesPerPartition;

        for (int i = 0; i < gridSize; i++) {
            ExecutionContext context = new ExecutionContext();
            context.putInt("startLine", start);
            context.putInt("endLine", end);
            context.putString("partitionName", start+ "-" +end);

            partitionMap.put("startLine" + i, context);

            start = end + 1;
            end += linesPerPartition;
        }

        System.out.println("Partition Map:"+partitionMap);
        return partitionMap;
    }
}
