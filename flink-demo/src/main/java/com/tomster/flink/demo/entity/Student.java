package com.tomster.flink.demo.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author meihewang
 * @date 2021/06/02  13:41
 * tom 12 m 1
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Student {

    private String name;

    private int age;

    private String gender;

    private int classNo;

}
