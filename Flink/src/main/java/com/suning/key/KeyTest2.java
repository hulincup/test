package com.suning.key;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author lynn
 * zs female 21 3 77 88 99
 * lisi male 21 4 77 55 99
 * wangwu female 5 78 79 45
 */
public class KeyTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSream = environment.socketTextStream("bd1301", 7777);

        SingleOutputStreamOperator<Student> map = dataSream.map(new MapFunction<String, Student>() {
            public Student map(String value) throws Exception {
                String[] word = value.split(" ");
                String name = word[0];
                String gender = word[1];
                Integer age = Integer.parseInt(word[2]);
                String grade = word[3];
                Integer math = Integer.parseInt(word[4]);
                Integer chinese = Integer.parseInt(word[5]);
                Integer english = Integer.parseInt(word[6]);
                // return new Student(name,gender,age,new Tuple2<String, Tuple3<Integer, Integer, Integer>>
                //        (grade,new Tuple3<Integer,Integer,Integer>(math,chinese,english)));
                return new Student(name, gender, age, new Tuple2(grade, new Tuple3(math, chinese, english)));
            }
        });

        SingleOutputStreamOperator<Student> result = map.keyBy("age").reduce(new ReduceFunction<Student>() {
            public Student reduce(Student student1, Student student2) throws Exception {
                String name = student1.getName() + student2.getName();
                String gender = student1.getGender() + student2.getGender();
                Integer age = student1.getAge() + student2.getAge();
                String grade = student1.getGradeAndScore().f0 + student2.getGradeAndScore().f0;
                Tuple3<Integer, Integer, Integer> tuple3 = new Tuple3<Integer, Integer, Integer>(
                        student1.getGradeAndScore().f1.f0 + student2.getGradeAndScore().f1.f0,
                        student1.getGradeAndScore().f1.f1 + student2.getGradeAndScore().f1.f1,
                        student1.getGradeAndScore().f1.f2 + student2.getGradeAndScore().f1.f2
                );
                //Tuple2<String, Tuple3> tuple2 = new Tuple2<String, Tuple3>(grade, tuple3);
                Tuple2<String, Tuple3<Integer, Integer, Integer>> tuple2 = new Tuple2<String, Tuple3<Integer, Integer, Integer>>(grade, tuple3);
                return new Student(name, gender, age, tuple2);
            }
        });

        result.print();

        environment.execute("KeyTest2");
    }
}
