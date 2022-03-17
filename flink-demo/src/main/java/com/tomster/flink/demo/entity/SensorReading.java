package com.tomster.flink.demo.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author meihewang
 * @date 2021/06/01  11:22
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {

    private String sensor;

    private long ts;

    private double temperature;

}
