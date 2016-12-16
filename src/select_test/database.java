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
	public database(){
		this.connect();
	}
	public void connect(){
        String url = "jdbc:mysql://localhost:3306/"+this.db;
        String user = this.user;
        String password = this.password;
		try {
            this.connection = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
		    e.printStackTrace();
		} catch (Exception e) {
		    System.out.println(e.getMessage());
		} 
	}
	public void query(String query) throws SQLException{
        Statement st = null;
        ResultSet rs = null;
        st = this.dbFlow().createStatement();
        rs = st.executeQuery(query);
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
