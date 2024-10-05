package com.rockwxp.dataprocess.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author rock
 * @date 2024/10/1 14:18
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableProcess {
    // 数据源表
    String sourceTable;

    // 操作类型
    String operateType;

    // 目标表
    String sinkTable;

    // 列族
    String sinkFamily;

    // 字段
    String sinkColumns;

    // HBASE 建表 rowKey
    String sinkRowKey;

}
