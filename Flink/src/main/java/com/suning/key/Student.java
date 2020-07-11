package com.suning.key;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @author lynn
 */
public class Student {
    private String name;
    private String gender;
    private Integer age;
    private Tuple2<String, Tuple3<Integer,Integer,Integer>> gradeAndScore;

    public Student() {
    }

    public Student(String name, String gender, Integer age, Tuple2<String, Tuple3<Integer, Integer, Integer>> gradeAndScore) {
        this.name = name;
        this.gender = gender;
        this.age = age;
        this.gradeAndScore = gradeAndScore;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Tuple2<String, Tuple3<Integer, Integer, Integer>> getGradeAndScore() {
        return gradeAndScore;
    }

    public void setGradeAndScore(Tuple2<String, Tuple3<Integer, Integer, Integer>> gradeAndScore) {
        this.gradeAndScore = gradeAndScore;
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", gender='" + gender + '\'' +
                ", age=" + age +
                ", gradeAndScore=" + gradeAndScore +
                '}';
    }
}
