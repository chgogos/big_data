package gr.chgogos;

public class Student{
    String name;
    double score;
    int gradYear;

    Student(String name, double score, int gradYear){
        this.name=name;
        this.score=score;
        this.gradYear=gradYear;
    }

    public String toString(){
        return String.format("%20.20s %.2f %d", name , score,gradYear);
    }
  }