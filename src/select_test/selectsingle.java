package select_test;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.SQLException;
import java.util.Scanner;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.*;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
public class selectsingle implements Runnable{
	private URL url;
	private URLConnection con;
	private HttpURLConnection http;
	private String url_request,db_type,query,request;
	private OutputStreamWriter os;
	private Thread t;
	private long start_time;
	private long end_time;
	private int debug=0;
	private int left_edge;
	private File file;
	private int right_edge;
	private int thd_id;
	private int start_line;
	private database mysql;
	
	public void startTimer(String s){
		start_time=System.currentTimeMillis();
    	System.out.println(s);
	}
	
	public void timetoExecute(String s){
		end_time=System.currentTimeMillis();
        System.out.println(s+(end_time-start_time));
	}
	
	
	private void edge_calc(){
		this.left_edge=thd_id*start_line;
		this.right_edge=start_line+left_edge;
		System.out.println("left_edge:"+this.left_edge+"thd_id:"+thd_id);
		System.out.println("rigth_edge:"+this.right_edge+"thd_id"+thd_id);
	}
	
	public selectsingle(String r,int d, File f,int id,int start_line) throws IOException{
		mysql=new database();
		request=r;
		debug=d;
		file=f;
		this.thd_id=id;
		this.start_line=start_line;
		edge_calc();
		/*
		this.query=query;
		url = new URL(request);	
		con = url.openConnection();
		http = (HttpURLConnection)con;
		http.setRequestMethod("POST"); 
		http.setDoOutput(true);
		http.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
		os= new OutputStreamWriter(http.getOutputStream());
	    */
	}

	public void run(){
		
	    startTimer("Start thread: "+Integer.toString(thd_id));
	    
	    int row_index=0;
	    @SuppressWarnings("resource")
		Scanner scanner=null;
		try {
			scanner = new Scanner(file);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

 		System.out.println("left edge:"+this.left_edge);
	    while (row_index<this.left_edge){
        	scanner.nextLine();
        	row_index++;
        }
	    String sqlstart="INSERT INTO `keywords` ( `keyword`, `competetion`, `ams`, `lms`, `impressions`, `clicks`, `cost`, `ctr`, `country`, `average_cpc`, `average_position`, `cpc`, `m1`, `m2`, `m3`, `m4`, `m5`, `m6`, `m7`, `m8`, `m9`, `m10`, `m11`, `m12`) VALUES ";
	    String query="";
	    String api_output="";

		int i=0;
		KeywordLookup response;
	    database mysql=new database();
		Gson gson = new Gson();
		boolean first_input=false;
        while (row_index<=this.right_edge) {
             String line = scanner.nextLine().trim();
             if(!line.equals("")){
            	 try { Thread.sleep(500);} catch (InterruptedException e1) {}
		         query=sqlstart;
		         System.out.println("Analizzo:"+line+" - thread attivo id:"+thd_id);
				 try (java.util.Scanner s = new java.util.Scanner(new java.net.URL(request+line+"&loc=italy").openStream())) {
					 api_output=s.useDelimiter("\\A").next();
					 System.out.println("ANALIZZO "+line);
					 if(api_output!=null&&!api_output.equals("null")){
						 response=gson.fromJson(api_output, KeywordLookup.class);
						 i=0;
						 for(HashMap.Entry<String, KeywordStat> entry : response.keywords.entrySet()) {
						    String key = entry.getKey();
						    KeywordStat value = entry.getValue();
						    if(value!=null){
								if(i!=0) query+=", ";
								query+="('"+value.keyword+"', "+
								 	value.competetion+", "+
			                        value.ams+", "+
			                        value.lms+", "+
			                        value.Estimated_Impressions+", "+
			                        value.Estimated_clicks+", "+
			                        value.Estimated_Cost+", "+
			                        value.Estimated_CTR+",  "+
			                        "'"+value.country+"', "+
			                        value.Estimated_Average_CPC+", "+
			                        value.Estimated_Average_Position+", "+
			                        value.cpc+", "+
			                        value.m1+", "+
			                        value.m2+", "+
			                        value.m3+", "+
			                        value.m4+", "+
			                        value.m5+", "+
			                        value.m6+", "+
			                        value.m7+", "+
			                        value.m8+", "+
			                        value.m9+", "+
			                        value.m10+", "+
			                        value.m11+", "+
			                        value.m12+")"+ System.lineSeparator();
									i++;
						     }
						    // do what you have to do here
						    // In your case, an other loop.
						 }
						 if(i>0){
							 query+=" ON DUPLICATE KEY UPDATE keyword=keyword";
							 mysql.query(query);
						 }
					 }
					 
					 System.out.println("FINISCO ed inserisco nel db la parola: "+line);
				 } catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				 } catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				 } catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				 }
	           	}
	        	row_index++;
	        }
		System.out.println("TERMINATO "+row_index);
	}
	
	
	public void start (){
		t=new Thread(this);
		t.start();
	}

	public void wait_done() {
		// TODO Auto-generated method stub
		try {
			t.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
}
