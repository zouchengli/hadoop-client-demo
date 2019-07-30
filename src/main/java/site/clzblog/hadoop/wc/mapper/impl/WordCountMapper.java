package site.clzblog.hadoop.wc.mapper.impl;

import site.clzblog.hadoop.wc.context.Context;
import site.clzblog.hadoop.wc.mapper.Mapper;

public class WordCountMapper implements Mapper {
    public void map(String line, Context context) {
        String[] words = line.split(" ");
        for (String word : words) {
            Object value = context.get(word);
            if (null == value) context.write(word, 1);
            else {
                int count = (Integer) value;
                context.write(word, count);
            }
        }
    }
}
