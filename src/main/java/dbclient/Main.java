package dbclient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.*;



public class Main {
	
	private static Connection con = null;
	private static PreparedStatement pstmt = null;  
	private static Logger log = Logger.getLogger(Main.class);
	
	private static final String DB_URL = "jdbc:hsqldb:hsql://127.0.0.1:9001/test-db";
	private static final String LOGIN = "SA";
	private static final String PASSWORD = "";
	
	private static void connect() throws SQLException{
		con = DriverManager.getConnection(
				DB_URL,
				LOGIN,
				PASSWORD);
	}
	
	private static void createtable(String sql) throws SQLException{
		pstmt = con.prepareStatement(sql);
		pstmt.execute();	
	}
	
	private static void  dropTable(String tableNme) throws SQLException{
		pstmt = con.prepareStatement("DROP TABLE "+tableNme+" IF EXISTS CASCADE"); 
		pstmt.execute();
	}
	
	private static void  insertStudent(int pkey, String name, String sex, int age, int level) throws SQLException{
		pstmt = con.prepareStatement(
                "INSERT INTO Student" +
                " VALUES (?, ?, ?, ?, ?);"
                );
		pstmt.setInt(1, pkey);
		pstmt.setString(2, name);
		pstmt.setString(3, sex);
		pstmt.setInt(4, age);
		pstmt.setInt(5, level);
		pstmt.execute();
	}
	
	private static void  insertFaculty(int pkey, String name) throws SQLException{
		pstmt = con.prepareStatement(
                "INSERT INTO Faculty" +
                " VALUES (?, ?);"
                );
		pstmt.setInt(1, pkey);
		pstmt.setString(2, name);
		pstmt.execute();	
	}
	
	private static void  insertClass(int pkey, String name, int fkey) throws SQLException{
		pstmt = con.prepareStatement(
                "INSERT INTO Class" +
                " VALUES (?, ?, ?);"
                );
		pstmt.setInt(1, pkey);
		pstmt.setString(2, name);
		pstmt.setInt(3, fkey);
		pstmt.execute();
		
	}
	
	private static void  insertEnrollment(int fkey_student, int fkey_class) throws SQLException{
		pstmt = con.prepareStatement(
                "INSERT INTO Enrollment" +
                " VALUES (?, ?);"
                );
		pstmt.setInt(1, fkey_student);
		pstmt.setInt(2, fkey_class);
		pstmt.execute();
		
	}

	private static ResultSet executeQuery(String sql) throws SQLException{
		
		pstmt = con.prepareStatement(sql);
	    return pstmt.executeQuery();
	}
	
	private static void task1() throws SQLException{

	    ResultSet rs = executeQuery("SELECT pkey, name FROM Student");
	    
	    String result="\nWynik zapytania 1:\n";

	    while (rs.next()) {
	        int x = rs.getInt("pkey");
	        String s = rs.getString("name");
	        s = s.split(" ")[1];
	        
	        result += "pkey="+x+ " "+"name="+s+" "+"\n";
	    }
	    log.info(result);
	}
	
	private static void task2() throws SQLException{
	    
	    ResultSet rs = executeQuery("SELECT s.pkey, s.name "
		    	  + "FROM Student s LEFT JOIN Enrollment e ON (s.pkey=e.fkey_student) "
		    	  + "WHERE e.fkey_student IS NULL");
	    
	    String result="\n\nWynik zapytania 2:\n";

	    while (rs.next()) {
	        int x = rs.getInt("pkey");
	        String s = rs.getString("name");
	        s = s.split(" ")[1];
	        
	        result += "pkey="+x+ " "+"name="+s+" "+"\n"; 
	    }
	    
	    log.info(result);
	}
	
	private static void task3() throws SQLException{
	    
	    ResultSet rs = executeQuery("SELECT s.pkey, s.name "
		    	  + "FROM Student s LEFT JOIN Enrollment e ON (s.pkey=e.fkey_student) "
		    	  + "JOIN Class c ON (e.fkey_class=c.pkey)"
		    	  + "WHERE s.sex='female' AND c.name='Existentialism in 20th century'");
	    
	    String result="\n\nWynik zapytania 3:\n";

	    while (rs.next()) {
	        int x = rs.getInt("pkey");
	        String s = rs.getString("name");
	        s = s.split(" ")[1];
	        
	        result += "pkey="+x+ " "+"name="+s+" "+"\n";
	        
	    }
	    log.info(result);
	}
	
	private static void task4() throws SQLException{
	    
	    ResultSet rs = executeQuery("SELECT f.name "
		    	  + "FROM Student s FULL JOIN Enrollment e ON (s.pkey=e.fkey_student) "
		    	  + "FULL JOIN Class c ON (e.fkey_class=c.pkey)"
		    	  + " FULL JOIN Faculty f ON (c.fkey_faculty=f.pkey)"
		    	  + "WHERE c.pkey NOT IN (SELECT fkey_class FROM Enrollment)");
	    
	    String result="\n\nWynik zapytania 4:\n";

	    while (rs.next()) {
	        
	        String s = rs.getString("name");
	        
	        
	        result += "name="+s+" "+"\n";
	        
	    }
	    log.info(result);
		
	}
	
	private static void task5() throws SQLException{
	    
	    ResultSet rs = executeQuery("SELECT MAX(s.age) maxAge "
		    	  + "FROM Student s FULL JOIN Enrollment e ON (s.pkey=e.fkey_student) "
		    	  + "FULL JOIN Class c ON (e.fkey_class=c.pkey)"
		    	  + "WHERE c.name='Introduction to labour law'");
	    
	    String result="\n\nWynik zapytania 5:\n";

	    while (rs.next()) {
	        
	        int s = rs.getInt("maxAge");
	        
	        
	        result += "max_age="+s+" "+"\n";
	        
	    }
	    log.info(result);
	    
	   
		
	}
	
	private static void task6() throws SQLException{
	    
	    ResultSet rs = executeQuery("SELECT c.name, COUNT(e.fkey_student) countFk "
		    	  + "FROM Enrollment e  "
		    	  + "FULL JOIN Class c ON (e.fkey_class=c.pkey)"
		    	  + "GROUP BY c.name "
		    	  + "HAVING COUNT(e.fkey_student)>=2  ");
	    
	    String result="\n\nWynik zapytania 6:\n";

	    while (rs.next()) {
	        
	        String s = rs.getString("name");
	        
	        result += "name="+s+" "+"\n";
	        
	    }
	    log.info(result);
	}
	
	private static void task7() throws SQLException{
	    
	    ResultSet rs = executeQuery("SELECT s.level, AVG(s.age) av "
		    	  + "FROM Student s "
		    	  + "GROUP BY s.level ");
	    
	    String result="\n\nWynik zapytania 7:\n";

	    while (rs.next()) {
	        
	        int s = rs.getInt("level");
	        double a = rs.getDouble("av");
	        
	        
	        result += "level="+s+" "+"avg="+a+"\n";
	        
	    }
	    log.info(result);
		
	}
	

//-------main-------------------------------------------------------------	
	
	public static void main(String[] args) {
		
		BasicConfigurator.configure();

		try {
			
			connect();
			
			dropTable("Student");
			
			createtable(
                    "CREATE TABLE Student( " +
                    "pkey INT NOT NULL," +
                    "name VARCHAR(50) NOT NULL," +
                    "sex VARCHAR(6) NOT NULL," +
                    "age INT NOT NULL," +
                    "level INT NOT NULL," +
                    "PRIMARY KEY (pkey) );"
                    );
			
			
			
			insertStudent(1, "John Smith", "male", 23, 2);
			insertStudent(2, "Rebecca Milson", "female", 27, 3);
			insertStudent(3, "George Heartbreaker", "male", 19, 1);
			insertStudent(4, "Deepika Chopra", "female", 25, 3);
			
//----------Faculty-----------------------------------------------------
			  
			dropTable("Faculty");
			
			pstmt = con.prepareStatement(
	                    "CREATE TABLE Faculty( " +
	                    "pkey INT NOT NULL," +
	                    "name VARCHAR(50) NOT NULL," +
	                    "PRIMARY KEY (pkey) );"
	                    );
			pstmt.execute();
				
			insertFaculty(100, "Engineering");
			insertFaculty(101, "Philosophy");
			insertFaculty(102, "Law and administration");
			insertFaculty(103, "Languages");
				
//----------Class-----------------------------------------------------
			
			dropTable("Class");
				
			pstmt = con.prepareStatement(
	                    "CREATE TABLE Class( " +
	                    "pkey INT NOT NULL," +
	                    "name VARCHAR(50) NOT NULL," +
	                    "fkey_faculty INT NOT NULL," +
	                    "PRIMARY KEY (pkey),"+
	                    "FOREIGN KEY (fkey_faculty) REFERENCES Faculty (pkey));"
	                    );
			pstmt.execute();
			
			insertClass(1000, "Introduction to labour law", 102);
			insertClass(1001, "Graph algorithms", 100); 
			insertClass(1002, "Existentialism in 20th century", 101);
			insertClass(1003, "English grammar", 103);
			insertClass(1004, "From Plato to Kant", 101);

//--------Enrollment-------------------------------------------------------
			
			dropTable("Enrollment");
				
			pstmt = con.prepareStatement(
	                    "CREATE TABLE Enrollment( " +
	                    "fkey_student INT NOT NULL," +
	                    "fkey_class INT NOT NULL," +
	                    "FOREIGN KEY (fkey_class) REFERENCES Class (pkey),"+
	                    "FOREIGN KEY (fkey_student) REFERENCES Student (pkey));"
	                    );			
			pstmt.execute();
			
			insertEnrollment(1,1000);
			insertEnrollment(1,1002);
			insertEnrollment(1,1003);
			insertEnrollment(1,1004);
			insertEnrollment(2,1002);
			insertEnrollment(2,1003);
			insertEnrollment(4,1000);
			insertEnrollment(4,1002);
			insertEnrollment(4,1003);

					
//----------SELECT---------------------------------------------
				
				task1();
				task2();
				task3();
				task4();
				task5();
				task6();
				task7();

			
		} catch (SQLException e) {

			log.error(e.getMessage());
		}
	}
}
