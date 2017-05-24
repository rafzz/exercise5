package dbclient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.*;



public class Main {
	
	private static Connection con = null;
	private static PreparedStatement pstmt = null;  
	
	private static Logger log = Logger.getLogger(Main.class);

	public static void main(String[] args) {
		
		BasicConfigurator.configure();
		
		
		try {
			con = DriverManager.getConnection(
			        "jdbc:hsqldb:hsql://127.0.0.1:9001/test-db",
			        "SA",
			        "");
			
			pstmt = con.prepareStatement("DROP TABLE Student IF EXISTS CASCADE"); 
			pstmt.execute();
			
			pstmt = con.prepareStatement(
                    "CREATE TABLE Student( " +
                    "pkey INT NOT NULL," +
                    "name VARCHAR(50) NOT NULL," +
                    "sex VARCHAR(6) NOT NULL," +
                    "age INT NOT NULL," +
                    "level INT NOT NULL," +
                    "PRIMARY KEY (pkey) );"
                    );
			
			pstmt.execute();
			
			pstmt = con.prepareStatement(
                    "INSERT INTO Student "+
                    "VALUES (1, 'John Smith', 'male', 23, 2);"
                    );
			
			pstmt.execute();
			
			pstmt = con.prepareStatement(
                    "INSERT INTO Student "+
                    "VALUES (2, 'Rebecca Milson', 'female', 27, 3);"
                    );
		 

			pstmt.execute();
			
			pstmt = con.prepareStatement(
                    "INSERT INTO Student "+
                    "VALUES (3, 'George Heartbreaker', 'male', 19, 1);"
                    );
		 

			pstmt.execute();
			
			pstmt = con.prepareStatement(
                    "INSERT INTO Student "+
                    "VALUES (4, 'Deepika Chopra', 'female', 25, 3);"
                    );
		 

			pstmt.execute();
			
//---------------------------------------------------------------
			  
			pstmt = con.prepareStatement("DROP TABLE Faculty IF EXISTS CASCADE"); 
			pstmt.execute();
			
			 
			pstmt = con.prepareStatement(
	                    "CREATE TABLE Faculty( " +
	                    "pkey INT NOT NULL," +
	                    "name VARCHAR(50) NOT NULL," +
	                    "PRIMARY KEY (pkey) );"
	                    );
				
			pstmt.execute();
				
			pstmt = con.prepareStatement(
	                    "INSERT INTO Faculty "+
	                    "VALUES (100, 'Engineering');"
	                    );
				
			pstmt.execute();
				
			pstmt = con.prepareStatement(
	                    "INSERT INTO Faculty "+
	                    "VALUES (101, 'Philosophy');"
	                    );
			 

			pstmt.execute();
				
			pstmt = con.prepareStatement(
	                    "INSERT INTO Faculty "+
	                    "VALUES (102, 'Law and administration');"
	                    );
			 

			pstmt.execute();
				
			pstmt = con.prepareStatement(
	                    "INSERT INTO Faculty "+
	                    "VALUES (103, 'Languages');"
	                    );
			 

			pstmt.execute();
				
				
//---------------------------------------------------------------
			pstmt = con.prepareStatement("DROP TABLE Class IF EXISTS CASCADE"); 
			pstmt.execute();
				
				
			pstmt = con.prepareStatement(
	                    "CREATE TABLE Class( " +
	                    "pkey INT NOT NULL," +
	                    "name VARCHAR(50) NOT NULL," +
	                    "fkey_faculty INT NOT NULL," +
	                    "PRIMARY KEY (pkey),"+
	                    "FOREIGN KEY (fkey_faculty) REFERENCES Faculty (pkey));"
	                    );
			    
			pstmt.execute();
			    
			pstmt = con.prepareStatement(
	                    "INSERT INTO Class "+
	                    "VALUES (1000, 'Introduction to labour law', 102);"
	                    );
			pstmt.execute();
			    
			pstmt = con.prepareStatement(
	                    "INSERT INTO Class "+
	                    "VALUES (1001, 'Graph algorithms', 100);"
	                    );
			pstmt.execute();
			    
			pstmt = con.prepareStatement(
	                    "INSERT INTO Class "+
	                    "VALUES (1002, 'Existentialism in 20th century', 101);"
	                    );
			    
			pstmt.execute();
			    
			pstmt = con.prepareStatement(
	                    "INSERT INTO Class "+
	                    "VALUES (1003, 'English grammar', 103);"
	                    );
			    
			pstmt.execute();
			    
			pstmt = con.prepareStatement(
	                    "INSERT INTO Class "+
	                    "VALUES (1004, 'From Plato to Kant', 101);"
	                    );
			    
			pstmt.execute();

//---------------------------------------------------------------
			
			pstmt = con.prepareStatement("DROP TABLE Enrollment IF EXISTS CASCADE"); 
			pstmt.execute();
				
				
			pstmt = con.prepareStatement(
	                    "CREATE TABLE Enrollment( " +
	                    "fkey_student INT NOT NULL," +
	                    "fkey_class INT NOT NULL," +
	                    "FOREIGN KEY (fkey_class) REFERENCES Class (pkey),"+
	                    "FOREIGN KEY (fkey_student) REFERENCES Student (pkey));"
	                    );			
			
			pstmt.execute();
			
			pstmt = con.prepareStatement(
                    "INSERT INTO Enrollment "+
                    "VALUES (1, 1000);"
                    );
		    
			pstmt.execute();
			
			pstmt = con.prepareStatement(
                    "INSERT INTO Enrollment "+
                    "VALUES (1, 1002);"
                    );
		    
			pstmt.execute();
			
			pstmt = con.prepareStatement(
                    "INSERT INTO Enrollment "+
                    "VALUES (1, 1003);"
                    );
		    
			pstmt.execute();
			
			pstmt = con.prepareStatement(
                    "INSERT INTO Enrollment "+
                    "VALUES (1, 1004);"
                    );
		    
			pstmt.execute();
			
			pstmt = con.prepareStatement(
                    "INSERT INTO Enrollment "+
                    "VALUES (2, 1002);"
                    );
		    
			pstmt.execute();
			
			pstmt = con.prepareStatement(
                    "INSERT INTO Enrollment "+
                    "VALUES (2, 1003);"
                    );
		    
			pstmt.execute();
			
			pstmt = con.prepareStatement(
                    "INSERT INTO Enrollment "+
                    "VALUES (4, 1000);"
                    );
		    
			pstmt.execute();
			
			pstmt = con.prepareStatement(
                    "INSERT INTO Enrollment "+
                    "VALUES (4, 1002);"
                    );
		    
			pstmt.execute();
			
			pstmt = con.prepareStatement(
                    "INSERT INTO Enrollment "+
                    "VALUES (4, 1003);"
                    );
		    
			pstmt.execute();
			
			
				
//----------SELECT---------------------------------------------
				

			
				pstmt = con.prepareStatement(
						"SELECT pkey, name FROM Student");
			    ResultSet rs = pstmt.executeQuery();
			    
			    String result="\nWynik zapytania 1:\n";

			    while (rs.next()) {
			        int x = rs.getInt("pkey");
			        String s = rs.getString("name");
			        s = s.split(" ")[1];
			        
			        result += "pkey="+x+ " "+"name="+s+" "+"\n";
			        
			    }
			    log.info(result);
			    
			    
			    pstmt = con.prepareStatement(
			    		"SELECT s.pkey, s.name "
			    	  + "FROM Student s LEFT JOIN Enrollment e ON (s.pkey=e.fkey_student) "
			    	  + "WHERE e.fkey_student IS NULL");
			    
			    rs = pstmt.executeQuery();
			    
			    result="\n\nWynik zapytania 2:\n";

			    while (rs.next()) {
			        int x = rs.getInt("pkey");
			        String s = rs.getString("name");
			        s = s.split(" ")[1];
			        
			        result += "pkey="+x+ " "+"name="+s+" "+"\n";
			        
			    }
			    
			    log.info(result);
			    
			    pstmt = con.prepareStatement(
			    		"SELECT s.pkey, s.name "
			    	  + "FROM Student s LEFT JOIN Enrollment e ON (s.pkey=e.fkey_student) "
			    	  + "JOIN Class c ON (e.fkey_class=c.pkey)"
			    	  + "WHERE s.sex='female' AND c.name='Existentialism in 20th century'");
			    
			    rs = pstmt.executeQuery();
			    
			    result="\n\nWynik zapytania 3:\n";

			    while (rs.next()) {
			        int x = rs.getInt("pkey");
			        String s = rs.getString("name");
			        s = s.split(" ")[1];
			        
			        result += "pkey="+x+ " "+"name="+s+" "+"\n";
			        
			    }
			    log.info(result);
			    
			    pstmt = con.prepareStatement(
			    		"SELECT f.name "
			    	  + "FROM Student s FULL JOIN Enrollment e ON (s.pkey=e.fkey_student) "
			    	  + "FULL JOIN Class c ON (e.fkey_class=c.pkey)"
			    	  + " FULL JOIN Faculty f ON (c.fkey_faculty=f.pkey)"
			    	  + "WHERE c.pkey NOT IN (SELECT fkey_class FROM Enrollment)"
			    	  );
			    
			    rs = pstmt.executeQuery();
			    
			    result="\n\nWynik zapytania 4:\n";

			    while (rs.next()) {
			        
			        String s = rs.getString("name");
			        
			        
			        result += "name="+s+" "+"\n";
			        
			    }
			    log.info(result);
			    
			    pstmt = con.prepareStatement(
			    		"SELECT MAX(s.age) maxAge "
			    	  + "FROM Student s FULL JOIN Enrollment e ON (s.pkey=e.fkey_student) "
			    	  + "FULL JOIN Class c ON (e.fkey_class=c.pkey)"
			    	  + "WHERE c.name='Introduction to labour law'"
			    	  );
			    
			    rs = pstmt.executeQuery();
			    
			    result="\n\nWynik zapytania 5:\n";

			    while (rs.next()) {
			        
			        int s = rs.getInt("maxAge");
			        
			        
			        result += "max_age="+s+" "+"\n";
			        
			    }
			    log.info(result);

			    
			    pstmt = con.prepareStatement(
			    		"SELECT c.name, COUNT(e.fkey_student) countFk "
			    	  + "FROM Enrollment e  "
			    	  + "FULL JOIN Class c ON (e.fkey_class=c.pkey)"
			    	  + "GROUP BY c.name "
			    	  + "HAVING COUNT(e.fkey_student)>=2  "
			    	  );
			    
			    rs = pstmt.executeQuery();
			    
			    result="\n\nWynik zapytania 6:\n";

			    while (rs.next()) {
			        
			        String s = rs.getString("name");
			        
			        
			        result += "name="+s+" "+"\n";
			        
			    }
			    log.info(result);
			    
			    
			    pstmt = con.prepareStatement(
			    		"SELECT s.level, AVG(s.age) av "
			    	  + "FROM Student s "
			    	  + "GROUP BY s.level "
			    	  );
			    
			    rs = pstmt.executeQuery();
			    
			    result="\n\nWynik zapytania 7:\n";

			    while (rs.next()) {
			        
			        int s = rs.getInt("level");
			        double a = rs.getDouble("av");
			        
			        
			        result += "level="+s+" "+"avg="+a+"\n";
			        
			    }
			    log.info(result);
			    
			   
			    
			   
			
			
		} catch (SQLException e) {

			e.printStackTrace();
		}
		

	}

}
