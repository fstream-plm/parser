package select_test;

/*
user: spider_user
config spider: EQ46FP7CW0gz3Rlu
database: spider_db
*/


import java.sql.*;
public class database {
	private String user="spider_user";
	private String host="localhost";
	private String password="EQ46FP7CW0gz3Rlu";
	private String db="spider_db";
	private Connection connection = null;
    Statement st = null;
	public database(){
		this.connect();
        try {
			this.st=dbFlow().createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void connect(){
        String url = "jdbc:mysql://localhost:3306/"+this.db;
        String user = this.user;
        String password = this.password;
		try {
            this.connection = DriverManager.getConnection(url, user, password);
            this.connection.setAutoCommit(false);
		} catch (SQLException e) {
		    e.printStackTrace();
		} catch (Exception e) {
		    System.out.println(e.getMessage());
		} 
	}
	public void query(String query) throws SQLException{
		try {
			st.executeUpdate(query);
            
		} catch (Exception e) {
			e.printStackTrace();} 
	}
	private Connection dbFlow(){
		return this.connection;
	}
	public void disconnect(){
		try {
			dbFlow().close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
