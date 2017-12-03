package gr.chgogos;

import com.github.javafaker.*;

import etm.core.configuration.BasicEtmConfigurator;
import etm.core.configuration.EtmManager;
import etm.core.monitor.EtmMonitor;
import etm.core.monitor.EtmPoint;
import etm.core.renderer.SimpleTextRenderer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public class AppPlusMonitor {
  static EtmMonitor monitor;
  static int N = 10000;
  public static void main(String[] args) {
    BasicEtmConfigurator.configure();
    monitor = EtmManager.getEtmMonitor();
    monitor.start();

    Faker faker = new Faker();
    List<Student> students = new ArrayList<>();
    for(int i=0;i<N;i++)
        students.add(new Student(faker.name().fullName(), faker.number().randomDouble(2,5,10), faker.number().numberBetween(2015,2018))); 

    // for(Student st: students)
    //     System.out.println(st);
    
    for(int i=0;i<1000;i++){
        scenario1(students, 2017);
        scenario2(students, 2017);
        scenario3(students, 2017);
        scenario4(students, 2017);
        scenario5(students, 2017);
    }

    monitor.render(new SimpleTextRenderer());
    monitor.stop();
}

  static void scenario1(List<Student> students, int year){
        EtmPoint point = monitor.createPoint("scenario1");
        double highestScore = 0.0;
        Iterator<Student> iterator=students.iterator();
        while (iterator.hasNext()) {
            Student s = iterator.next();
            if (s.gradYear == year)
                if (s.score > highestScore)
                    highestScore = s.score;
        }
        System.out.println("#1. The highest score is " + highestScore);
        point.collect();
  }

    static void scenario2(List<Student> students, int year) {
        EtmPoint point = monitor.createPoint("scenario2");
        double highestScore = 0.0;
        for (Student s : students) {
            if (s.gradYear == year)
                if (s.score > highestScore)
                    highestScore = s.score;
        }
        System.out.println("#2. The highest score is " + highestScore);
        point.collect();
    }

   static void scenario3(List<Student> students, int year) {
        EtmPoint point = monitor.createPoint("scenario3");
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
        point.collect();
    }

    static void scenario4(List<Student> students, int year) {
        EtmPoint point = monitor.createPoint("scenario4");
        Optional<Double> highestScore = students.stream().filter(s -> s.gradYear == year).map(s -> s.score).max(Comparator.naturalOrder());
        System.out.println("#4. The highest score is " + highestScore.get());
        point.collect();
    }

    static void scenario5(List<Student> students, int year) {
        EtmPoint point = monitor.createPoint("scenario5");        
        Optional<Double> highestScore = students.parallelStream().filter(s -> s.gradYear == year).map(s -> s.score).max(Comparator.naturalOrder());
        System.out.println("#5. The highest score is " + highestScore.get());
        point.collect();
    }

}
