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
    migrate();
    
    return "index";
  }

  void migrate() {
   // if (!migrated) {
      // migrated = true;
      System.out.println("-------- MIGRATE --------- ");
      try (Connection connection = dataSource.getConnection()) {
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS profile (id uuid, name varchar, code varchar, preferences varchar, created_on timestamp DEFAULT NOW());");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS answer (profile_id uuid, question varchar, answer varchar, created_on timestamp DEFAULT NOW());");
        stmt.executeUpdate("CREATE UNIQUE INDEX IF NOT EXISTS idx_profile ON profile(id)");
        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_profile_name ON profile(name)");
        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_code ON profile(code)");
        stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_answer ON answer(profile_id, question)");
        System.out.println("-------- MIGRATE END --------- ");
      } catch(Exception e) {
        e.printStackTrace();
      }
    //}
  }
 
  @RequestMapping(value="/profile",
                method=RequestMethod.POST,
                consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  String create(@RequestBody MultiValueMap<String, String> formData, HttpServletRequest request) throws Exception {

    String token = readToken(request);
  
    try (Connection connection = dataSource.getConnection()) {    
      UUID id;
      String code = formData.get("code").get(0);

      PreparedStatement select = connection.prepareStatement("select id from profile where code = ?");
      select.setString(1, code);

      ResultSet rs = select.executeQuery();
      if (rs.next()) {
        id = (UUID)rs.getObject("id");
       
        if (!"test".equals("code")) {
          PreparedStatement countStmt = connection.prepareStatement("select count(*) count from answer where profile_id = ?");
          countStmt.setObject(1, id);

          ResultSet countrs = countStmt.executeQuery();
          if (countrs.next()) {
            int count = countrs.getInt("count");

            if (count > 0) {
            return "{ \"error\": \"" + code + " ha già partecipato!\"}";
            }
          }
        }
      } else {
        return "{ \"error\": \"codice errato!\"}";
      }
    
   
      PreparedStatement insert = connection.prepareStatement("update profile set name = ? where id = ?");
      
      insert.setString(1, String.valueOf(formData.get("name").get(0)));
      insert.setObject(2, id);
       
      insert.executeUpdate();

      return "{ \"redirectUrl\": \"/questions\", \"token\": \"" + id + "\"}";
    }
  }

  @RequestMapping("/questions")
  String getQuestions(HttpServletRequest request, Map<String, Object> model) {
    String token = readToken(request);

    System.out.println("token: " + token);

    model.put("profile_id", token);
     
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

    UUID profileId = UUID.fromString(token);

    try (Connection connection = dataSource.getConnection()) {
      

      PreparedStatement select = connection.prepareStatement("select * from answer where profile_id = ? and question = ?");
      
      PreparedStatement insert = connection.prepareStatement("insert into answer (profile_id, question, answer) values (?, ?, ?)");

      PreparedStatement update = connection.prepareStatement("update answer set answer = ? where profile_id = ? and question = ?");
    
      formData.keySet().stream().filter(k -> k.startsWith("q")).forEach(question -> {
        try {
          String answer = formData.get(question).toString();

          select.setObject(1, profileId);
          select.setString(2, question);
     
          ResultSet rs = select.executeQuery();

          if (rs.next()) {
            update.setString(1, answer);
            update.setObject(2, profileId);
            update.setString(3, question);

            update.executeUpdate();

            System.out.println("UPDATE " + profileId + " answered " +  answer + " to " + question);
          } else {      
            insert.setObject(1, profileId);
            insert.setString(2, question);
            insert.setString(3, answer);

            insert.executeUpdate();

            System.out.println("INSERT " + profileId + " answered " +  answer + " to " + question);
          }
        } catch (SQLException e) {
          e.printStackTrace();
        }
      });

      return "{ \"redirectUrl\": \"/bye\", \"token\": \"" + profileId.toString() + "\"}";
    }
  }

  @RequestMapping("/bye")
  String bye() {
    return "bye";
  }

  Integer calcScore(Profile profile, Profile candidate) {
    Integer score = candidate.answers.size();

    for(String q : profile.answers.keySet()) {
        String candidateA = candidate.answers.get(q);
        if(profile.answers.get(q).equals(candidateA)) {
          score += 10;
        }
    }
    return score;
  }

  void buildPreferences(Profile profile,  Map<UUID, Profile> candidates) throws Exception {
    class SortByScore implements Comparator<Score> {
      public int compare(Score a, Score b) {
          return b.score - a.score;
      }
    }

    System.out.println(candidates.size() + " candidates for " + profile.id);


    List<Score> scores = new ArrayList<>();
       
    List<UUID> candidateList = new ArrayList<UUID>();
    for(UUID candidateId : candidates.keySet()) {
      candidateList.add(candidateId);
    }
    Collections.shuffle(candidateList);


    for(UUID candidateId : candidateList) {
      Profile candidate = candidates.get(candidateId);
   
      if(candidate.id != profile.id) {
     
        Score score = new Score();

        score.profileId = candidate.id;
        score.profileName = candidate.name;
        score.score = calcScore(profile, candidate);
        score.score -= candidate.chosen;
      
        scores.add(score);
      }
    }

    System.out.println("Found " + scores.size() + " preferences for " + profile.id);

    List<Score> preferences = scores.stream().sorted(new SortByScore()).limit(3).collect(Collectors.toList());
    for(Score p : preferences) {
      Profile candidate = candidates.get(p.profileId);
      candidate.chosen += 1;
    }

    profile.preferences = preferences.stream().map(s -> s.profileName + " (" + s.score + ")").collect(Collectors.toList()).toString();

    try (Connection connection = dataSource.getConnection()) {    
      PreparedStatement update = connection.prepareStatement("update profile set preferences = ? where id = ?");
  
      update.setObject(1, profile.preferences);
      update.setObject(2, profile.id);
         
      update.executeUpdate();
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

  @RequestMapping(value="/admin/build-preferences",
                method=RequestMethod.POST)
  String buildPreferences() throws Exception {
    List<Profile> profiles = new ArrayList<Profile>();

    try (Connection connection = dataSource.getConnection()) {
      String query = "SELECT id, name from profile where name is not null and code != 'test' order by created_on";
      String answersQuery = "SELECT question, answer from answer where profile_id = ?";
      try (Statement stmt = connection.createStatement()) {
        PreparedStatement answersStmt = connection.prepareStatement(answersQuery);
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
          Profile profile = new Profile();
          profile.id = (UUID)rs.getObject("id");
          profile.name = rs.getString("name");
       
          answersStmt.setObject(1, profile.id);

          ResultSet answersRs = answersStmt.executeQuery();
          while (answersRs.next()) {
            String q = answersRs.getString("question");
            String a = answersRs.getString("answer");

            profile.answers.put(q, a);
          }

          profiles.add(profile);
        }
      }
    } catch(Exception e) {
      e.printStackTrace();
    }

    Map<UUID, Profile> candidates = new HashMap<UUID, Profile>();
    for(Profile profile: profiles) {
      candidates.put(profile.id, profile);
    }
  
   for(Profile profile: profiles) {
      buildPreferences(profile, candidates);
   }
 
    return "profiles";
  }


  @RequestMapping(value="/admin/profiles",
                method=RequestMethod.GET)
  String getProfiles(Map<String, Object> model) throws Exception {
    List<Profile> profiles = new ArrayList<Profile>();

    try (Connection connection = dataSource.getConnection()) {
      String query = "SELECT id, code, name, preferences from profile where name is not null and code != 'test' order by name";
      try (Statement stmt = connection.createStatement()) {
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
          Profile profile = new Profile();
          profile.id = (UUID)rs.getObject("id");
          profile.code = rs.getString("code");
          profile.name = rs.getString("name");
          profile.preferences = rs.getString("preferences");
          if (profile.preferences == null) {
            profile.preferences = "?";
          }
          profiles.add(profile);
        }
      }
    } catch(Exception e) {
      e.printStackTrace();
    }

    model.put("profiles", profiles);


    System.out.println("Found " + profiles.size() +  " profiles");

    return "profiles";
  }


  @RequestMapping(value="/admin/maintenance",
                method=RequestMethod.GET)
  String maintenance() throws Exception {
    return "maintenance";
  }

  @RequestMapping(value="/admin/codes",
                method=RequestMethod.POST, consumes="application/json")
  String importCodes(@RequestBody String codesString) throws Exception {  

    String[] codes = codesString.split("\\s+");
    System.out.println("codes " + codes);

    try (Connection connection = dataSource.getConnection()) {    
      PreparedStatement insert = connection.prepareStatement("insert into profile (id, code) values (?, ?)");
      
      for(String code : codes) {
        UUID id = java.util.UUID.randomUUID();

        insert.setObject(1, id);
        insert.setString(2, code);
       
        insert.executeUpdate();
      }

      return "index";
    } catch(Exception e) {
      e.printStackTrace();
    }

    return "index";
  }
  
/*
  @RequestMapping(value="/admin/reset",
                method=RequestMethod.POST)
  String reset() throws Exception {  
    System.out.println("RESET!!!!");
    try (Connection connection = dataSource.getConnection()) {
      Statement stmt = connection.createStatement();
      stmt.executeUpdate("DROP TABLE IF EXISTS profile;");
      stmt.executeUpdate("DROP TABLE IF EXISTS answer;");
      stmt.executeUpdate("DROP INDEX IF EXISTS idx_profile");
      stmt.executeUpdate("DROP INDEX IF EXISTS idx_profile_name");
      stmt.executeUpdate("DROP INDEX IF EXISTS idx_answer");
    } catch(Exception e) {
      e.printStackTrace();
    }

    migrate();
    
    return "index";
  }
*/

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
