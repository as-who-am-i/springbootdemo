package com.hbaseplus;

/**
 * @Prigram: com.hbaseplus
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-24 20:26
 */
public interface RowKeyGenerator {

    byte [] nextId();
}
