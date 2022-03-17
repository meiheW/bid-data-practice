package com.tomster.flink.demo.api.window.aggregateFunction;

import com.tomster.flink.demo.entity.Student;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author meihewang
 * @date 2022/01/05  10:19
 */
public class ClassMeanAgeAggregateFunction implements AggregateFunction<Student, Tuple2<Integer, Integer>, Double> {

    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        System.out.println("---createAccumulator---");
        return new Tuple2<>(0, 0);
    }

    @Override
    public Tuple2<Integer, Integer> add(Student student, Tuple2<Integer, Integer> accumulator) {
        System.out.println("---add---");
        return new Tuple2<>(student.getAge() + accumulator.f0, accumulator.f1 + 1);
    }

    @Override
    public Double getResult(Tuple2<Integer, Integer> accumulator) {
        System.out.println("---getResult---");
        return accumulator.f0 * 1.0 / accumulator.f1;
    }

    @Override
    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
    }
}
