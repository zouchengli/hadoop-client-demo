package site.clzblog.hadoop.wc.mapper;

import site.clzblog.hadoop.wc.context.Context;

public interface Mapper {
    void map(String line, Context context);
}
