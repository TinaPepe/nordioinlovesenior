package com.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.http.MediaType;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.Cookie;


import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.Collections;

import org.springframework.util.MultiValueMap;


@Controller
@SpringBootApplication
public class Main {

  @Value("${spring.datasource.url}")
  private String dbUrl;

  @Autowired
  private DataSource dataSource;

  // private boolean migrated = false;

  public static void main(String[] args) throws Exception {
    SpringApplication.run(Main.class, args);
  }
 
  @RequestMapping("/")
  String index() {
    // migrate();
    
    return "index";
  }

  void migrate() {
   // if (!migrated) {
      // migrated = true;
      System.out.println("-------- MIGRATE --------- ");
      try (Connection connection = dataSource.getConnection()) {
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS student (id uuid, name varchar, gender varchar, school_class varchar, gender_preference varchar, address varchar, preferences varchar, created_on timestamp DEFAULT NOW());");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS answer (student_id uuid, question varchar, answer varchar, created_on timestamp DEFAULT NOW());");
        stmt.executeUpdate("CREATE UNIQUE INDEX IF NOT EXISTS idx_student ON student(id)");
        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_student_name ON student(name)");
        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_answer ON answer(student_id, question)");
        System.out.println("-------- MIGRATE END --------- ");
      } catch(Exception e) {
        e.printStackTrace();
      }
    //}
  }
 
  @RequestMapping(value="/student",
                method=RequestMethod.POST,
                consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  String createStudent(@RequestBody MultiValueMap<String, String> formData, HttpServletRequest request) throws Exception {

    String token = readToken(request);

    List<String> genderPreferences = new ArrayList<String>();
      
    if (formData.get("ragazzo") != null) {
      genderPreferences.add("ragazzo");
    }
    if (formData.get("ragazza") != null) {
      genderPreferences.add("ragazza");
    }
    if (formData.get("nonbinary") != null) {
      genderPreferences.add("nonbinary");
    }
  
    try (Connection connection = dataSource.getConnection()) {    
      PreparedStatement select = connection.prepareStatement("select id from student where name =?");
      select.setString(1, String.valueOf(formData.get("name").get(0)));
      ResultSet rs = select.executeQuery();
      if (rs.next()) {
        UUID id = (UUID)rs.getObject("id");
       
        if (!id.toString().equals(token)) {
           return "{ \"error\": \"" + formData.get("name").get(0) + " ha già partecipato!\"}";
        } else {
          PreparedStatement update = connection.prepareStatement("update student set gender = ?, school_class = ?, gender_preference = ?, address = ? where id = ?");
      
          update.setString(1, formData.get("gender").get(0));
          update.setString(2, formData.get("school_class").get(0));
          update.setString(3, genderPreferences.stream().map(String::valueOf).collect(Collectors.joining(",")));
          update.setString(4, formData.get("address").get(0));
          update.setObject(5, id);
          
          update.executeUpdate();

          return "{ \"redirectUrl\": \"/questions\", \"token\": \"" + id + "\"}";
        }
      }
    
   
      PreparedStatement insert = connection.prepareStatement("insert into student (id, name, gender, school_class, gender_preference, address) values (?, ?, ?, ?, ?, ?)");
      
      UUID id = java.util.UUID.randomUUID();

      insert.setObject(1, id);
      insert.setString(2, String.valueOf(formData.get("name").get(0)));
      insert.setString(3, formData.get("gender").get(0));
      insert.setString(4, formData.get("school_class").get(0));
      insert.setString(5, genderPreferences.stream().map(String::valueOf).collect(Collectors.joining(",")));
      insert.setString(6, formData.get("address").get(0));
      
      insert.executeUpdate();

      return "{ \"redirectUrl\": \"/questions\", \"token\": \"" + id + "\"}";
    }
  }

  @RequestMapping("/questions")
  String getQuestions(HttpServletRequest request, Map<String, Object> model) {
    String token = readToken(request);

    System.out.println("token: " + token);

    model.put("student_id", token);
     
    return "questions";
  }

  String readToken(HttpServletRequest request) {
    Cookie cookie[]=request.getCookies();
    Cookie cook;
    String token="";
    if (cookie != null) {
      for (int i = 0; i < cookie.length; i++) {
        cook = cookie[i];
        if(cook.getName().equalsIgnoreCase("token"))
            token=cook.getValue();                  
      }    
    }
    return token;
  }

  @RequestMapping(value="/answers",
                method=RequestMethod.POST,
                consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  String saveAnswers(HttpServletRequest request, @RequestBody MultiValueMap<String, String> formData) throws Exception {

    String token = readToken(request);

    System.out.println("token: " + token);

    UUID studentId = UUID.fromString(token);

    try (Connection connection = dataSource.getConnection()) {
      

      PreparedStatement select = connection.prepareStatement("select * from answer where student_id = ? and question = ?");
      
      PreparedStatement insert = connection.prepareStatement("insert into answer (student_id, question, answer) values (?, ?, ?)");

      PreparedStatement update = connection.prepareStatement("update answer set answer = ? where student_id = ? and question = ?");
    
      formData.keySet().stream().filter(k -> k.startsWith("q")).forEach(question -> {
        try {
          String answer = formData.get(question).toString();

          select.setObject(1, studentId);
          select.setString(2, question);
     
          ResultSet rs = select.executeQuery();

          if (rs.next()) {
            update.setString(1, answer);
            update.setObject(2, studentId);
            update.setString(3, question);

            update.executeUpdate();

            System.out.println("UPDATE " + studentId + " answered " +  answer + " to " + question);
          } else {      
            insert.setObject(1, studentId);
            insert.setString(2, question);
            insert.setString(3, answer);

            insert.executeUpdate();

            System.out.println("INSERT " + studentId + " answered " +  answer + " to " + question);
          }
        } catch (SQLException e) {
          e.printStackTrace();
        }
      });

      return "{ \"redirectUrl\": \"/bye\", \"token\": \"" + studentId.toString() + "\"}";
    }
  }

  @RequestMapping("/bye")
  String bye() {
    return "bye";
  }

  Integer calcScore(Student student, Student candidate) {
    Integer score = candidate.answers.size();

    for(String q : student.answers.keySet()) {
        String candidateA = candidate.answers.get(q);
        if(student.answers.get(q).equals(candidateA)) {
          score += 10;
        }
    }
    return score;
  }

  void buildPreferences(Student student,  Map<UUID, Student> candidates) throws Exception {
    class SortByScore implements Comparator<Score> {
      public int compare(Score a, Score b) {
          return b.score - a.score;
      }
    }

    System.out.println(candidates.size() + " candidates for " + student.id + ", gender preferences: " + student.genderPreference);


    List<Score> scores = new ArrayList<>();

    String[] gps = student.genderPreference.split(",");
    List<String> genderPreference = new ArrayList<>();
    for(String gp : gps) {
      genderPreference.add(gp);
    }
       
    List<UUID> candidateList = new ArrayList<UUID>();
    for(UUID candidateId : candidates.keySet()) {
      candidateList.add(candidateId);
    }
    Collections.shuffle(candidateList);


    for(UUID candidateId : candidateList) {
      Student candidate = candidates.get(candidateId);
      System.out.println(candidate.id + " has gender " + candidate.gender);

      if(candidate.id != student.id && (genderPreference.contains(candidate.gender) || genderPreference.size() == 0)) {
     
        Score score = new Score();

        score.studentId = candidate.id;
        score.studentName = candidate.name;
        score.studentClass = candidate.schoolClass;
        score.score = calcScore(student, candidate);
        score.score -= candidate.chosen;
      
        scores.add(score);
      }
    }

    System.out.println("Found " + scores.size() + " preferences for " + student.id);

    List<Score> preferences = scores.stream().sorted(new SortByScore()).limit(3).collect(Collectors.toList());
    for(Score p : preferences) {
      Student candidate = candidates.get(p.studentId);
      candidate.chosen += 1;
    }

    student.preferences = preferences.stream().map(s -> s.studentName + " " + s.studentClass + " (" + s.score + ")").collect(Collectors.toList()).toString();

    try (Connection connection = dataSource.getConnection()) {    
      PreparedStatement update = connection.prepareStatement("update student set preferences = ? where id = ?");
  
      update.setObject(1, student.preferences);
      update.setObject(2, student.id);
         
      update.executeUpdate();
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  @RequestMapping(value="/admin/build-preferences",
                method=RequestMethod.POST)
  String buildPreferences() throws Exception {
    List<Student> students = new ArrayList<Student>();

    try (Connection connection = dataSource.getConnection()) {
      String query = "SELECT id, name, gender, school_class, gender_preference, address from student order by created_on";
      String answersQuery = "SELECT question, answer from answer where student_id = ?";
      try (Statement stmt = connection.createStatement()) {
        PreparedStatement answersStmt = connection.prepareStatement(answersQuery);
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
          Student student = new Student();
          student.id = (UUID)rs.getObject("id");
          student.name = rs.getString("name");
          student.schoolClass = rs.getString("school_class");
          student.gender = rs.getString("gender");
          student.genderPreference = rs.getString("gender_preference");

          answersStmt.setObject(1, student.id);

          ResultSet answersRs = answersStmt.executeQuery();
          while (answersRs.next()) {
            String q = answersRs.getString("question");
            String a = answersRs.getString("answer");

            student.answers.put(q, a);
          }

          students.add(student);
        }
      }
    } catch(Exception e) {
      e.printStackTrace();
    }

    Map<UUID, Student> candidates = new HashMap<UUID, Student>();
    for(Student student: students) {
      candidates.put(student.id, student);
    }
  
   for(Student student: students) {
      buildPreferences(student, candidates);
   }
 
    return "students";
  }


  @RequestMapping(value="/admin/students",
                method=RequestMethod.GET)
  String getStudents(Map<String, Object> model) throws Exception {
    List<Student> students = new ArrayList<Student>();

    try (Connection connection = dataSource.getConnection()) {
      String query = "SELECT id, name, gender, school_class, gender_preference, address, preferences from student order by school_class, name";
      try (Statement stmt = connection.createStatement()) {
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
          Student student = new Student();
          student.id = (UUID)rs.getObject("id");
          student.name = rs.getString("name");
          student.schoolClass = rs.getString("school_class");
          student.gender = rs.getString("gender");
          student.genderPreference = rs.getString("gender_preference");
          student.preferences = rs.getString("preferences");
          student.address = rs.getString("address");
          if (student.preferences == null) {
            student.preferences = "?";
          }
          students.add(student);
        }
      }
    } catch(Exception e) {
      e.printStackTrace();
    }

    model.put("students", students);


    System.out.println("Found " + students.size() +  " students");

    return "students";
  }


  @RequestMapping(value="/admin/maintenance",
                method=RequestMethod.GET)
  String maintenance() throws Exception {
    return "maintenance";
  }
  

  @RequestMapping(value="/admin/reset",
                method=RequestMethod.POST)
  String reset() throws Exception {  
    System.out.println("RESET!!!!");
    try (Connection connection = dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      stmt.executeUpdate("DROP TABLE IF EXISTS student;");
      stmt.executeUpdate("DROP TABLE IF EXISTS answer;");
      stmt.executeUpdate("DROP INDEX IF EXISTS idx_student");
      stmt.executeUpdate("DROP INDEX IF EXISTS idx_student_name");
      stmt.executeUpdate("DROP INDEX IF EXISTS idx_answer");
    } catch(Exception e) {
      e.printStackTrace();
    }

    migrate();
    
    return "index";
  }

  @RequestMapping(value="/admin/migrate",
                method=RequestMethod.POST)
  String runMigrate() throws Exception {  
 
    migrate();
    
    return "index";
  }

  @Bean
  public DataSource dataSource() throws SQLException {
    if (dbUrl == null || dbUrl.isEmpty()) {
      return new HikariDataSource();
    } else {
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(dbUrl);
      return new HikariDataSource(config);
    }
  }

}
