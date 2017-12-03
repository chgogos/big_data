package gr.chgogos;

import com.github.javafaker.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public class App {

  static int N = 1000000;
  public static void main(String[] args) {
    Faker faker = new Faker();
    List<Student> students = new ArrayList<>();
    for(int i=0;i<N;i++)
        students.add(new Student(faker.name().fullName(), faker.number().randomDouble(2,5,10), faker.number().numberBetween(2015,2018))); 

    // for(Student st: students)
    //     System.out.println(st);
    
    scenario1(students, 2017);
    scenario2(students, 2017);
    scenario3(students, 2017);
    scenario4(students, 2017);
  }

  static void scenario1(List<Student> students, int year){
        double highestScore = 0.0;
        Iterator<Student> iterator=students.iterator();
        while (iterator.hasNext()) {
            Student s = iterator.next();
            if (s.gradYear == year)
                if (s.score > highestScore)
                    highestScore = s.score;
        }
        System.out.println("#1. The highest score is " + highestScore);
  }

    static void scenario2(List<Student> students, int year) {
        double highestScore = 0.0;
        for (Student s : students) {
            if (s.gradYear == year)
                if (s.score > highestScore)
                    highestScore = s.score;
        }
        System.out.println("#2. The highest score is " + highestScore);
    }

   static void scenario3(List<Student> students, int year) {
        Optional<Double> highestScore = students.stream().filter(new Predicate<Student>() {
            @Override
            public boolean test(Student s) {
                return s.gradYear == year;
            }
        }).map(new Function<Student, Double>() {
            @Override
            public Double apply(Student s) {
                return s.score;
            }
        }).max(Comparator.naturalOrder());
        System.out.println("#3. The highest score is " + highestScore.get());
    }

    static void scenario4(List<Student> students, int year) {
        Optional<Double> highestScore = students.stream().filter(s -> s.gradYear == year).map(s -> s.score).max(Comparator.naturalOrder());
        System.out.println("#4. The highest score is " + highestScore.get());
    }
}
