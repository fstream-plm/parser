package select_test;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
public class service {
	public static void main(String[] args){
		System.out.println("Avvio test");
		int worker=Integer.parseInt(args[0]);
		String query=args[1];
		int debug=0,start_line=0;
		String file_path=args[3];
		
		if(args[2].equals("debug")){
			debug=1;
		}
		File file = new File(file_path);
		LineNumberReader lnr;
		try {
			lnr = new LineNumberReader(new FileReader(new File(file_path)));
			lnr.skip(Long.MAX_VALUE);
			start_line = lnr.getLineNumber()/worker;
			//System.out.println(start_line);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		select[] st=new select[worker];
		for (int i=0;i<worker;i++){
			try {
				st[i]=new select(query,debug,file,i,start_line);
				st[i].start();
			} catch (IOException e) {e.printStackTrace();}
		}
		// System.out.println("Test terminato");
	}
	
}
