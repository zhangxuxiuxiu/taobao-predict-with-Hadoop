package cn.edu.pku.zx.ali.predict.evaluate;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

public class LineWriter {
	public static void write(Iterable<String> lines, String outputFile) throws FileNotFoundException
	{
		File output=new File(outputFile);
		if(!output.exists())
			throw new FileNotFoundException();
		
		BufferedWriter writer=null;	
		try {
			writer = new BufferedWriter(new FileWriter(output));
			for(String line: lines)
			{
				writer.write(line+"\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally{
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}		
	}
}
