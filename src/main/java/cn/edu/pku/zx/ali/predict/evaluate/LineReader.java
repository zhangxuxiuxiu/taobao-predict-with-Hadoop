package cn.edu.pku.zx.ali.predict.evaluate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class LineReader {
	public static Iterable<String> read(String fileName) throws FileNotFoundException
	{
		if(null==fileName)
			throw new NullPointerException();
		File file=new File(fileName);
//		System.out.println(file.getAbsolutePath());
		if(!file.exists())
			throw new FileNotFoundException();
		
		List<String> lines=new LinkedList<String>();
		BufferedReader reader=null;	
		reader = new BufferedReader(new FileReader(file));
        String str = null;
        try {
			while ((str = reader.readLine()) != null) {
			    lines.add(str);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
        finally
        {
        	try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        	
		return lines;
	}
}
