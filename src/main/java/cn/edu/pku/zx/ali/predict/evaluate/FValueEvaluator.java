package cn.edu.pku.zx.ali.predict.evaluate;

import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class FValueEvaluator {
	private int hits, guesses;
	private Set<String> facts;

	public FValueEvaluator() {
		reset();
	}

	private void reset() {
		hits = 0;
		guesses = 0;
		if (null == facts)
			facts = new HashSet<String>();
		facts.clear();
	}

	public void evaluate(String resultSetFile, String outputFile,
			String subsetFile) throws FileNotFoundException {
		reset();
		Set<String> subsets = new HashSet<String>();
		// subsets items
		for (String line : LineReader.read(subsetFile))
			subsets.add(getItemAtIndex(line, 0));

		// reference set
		for (String item : LineReader.read(resultSetFile))
			if (subsets.contains(getItemAtIndex(item, 1)))
				facts.add(item);

		List<String> finalResult=new LinkedList<String>();
		// hits
		for (String guess : LineReader.read(outputFile)) {
			if (subsets.contains(getItemAtIndex(guess, 1))) {
				if (facts.contains(guess))
					++hits;
				++guesses;
				finalResult.add(guess);
			}
		}
		
		LineWriter.write(finalResult, "../finalResult/final.csv");
	}

	private static String getItemAtIndex(String line, int idx) {
		return line.split(",")[idx];
	}

	public double getValue() {
		return hits * 2.0 / (guesses + facts.size());
	}

	@Override
	public String toString() {
		String str = "";
		str += "hits=" + hits + "\tguesses=" + guesses + "\tfacts="
				+ facts.size() + "\tf1=" + getValue();
		return str;
	}

	public static void main(String[] argv) throws FileNotFoundException {
		final String outputFile = "../output/part-r-00000", resultSet = "../resultset/part-r-00000", subsetFile = "../subset/items.csv";
		FValueEvaluator evaluator = new FValueEvaluator();
		evaluator.evaluate(resultSet, outputFile, subsetFile);
		System.out.println(evaluator);
	}
}
