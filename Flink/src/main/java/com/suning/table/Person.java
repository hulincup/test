package com.suning.table;


/**
 * @author lynn
 */
public class Person {
    public String id;
    public String name;

    public int age;
    public int score;
    public String cls;//班级

    public Person(){

    }

    public Person(String id, String name, int age, int score,String cls) {
        this.id = id;
        this.name = name;
        this.cls = cls;
        this.age = age;
        this.score = score;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getCls() {
        return cls;
    }

    public void setCls(String cls) {
        this.cls = cls;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", cls='" + cls + '\'' +
                ", age=" + age +
                ", score=" + score +
                '}';
    }
}
