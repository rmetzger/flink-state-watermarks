package com.dataartisans.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Scanner;

/**
 * Created by robert on 2/29/16.
 */
public class YarnLogAggregationResultSplitter {

	/**
	 * Reads an aggregated YARN log and splits it into smaller files
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		ParameterTool pt = ParameterTool.fromArgs(args);
		Scanner sc = new Scanner(new FileInputStream(pt.getRequired("input")));
		FileOutputStream fos = new FileOutputStream("default");
		while(sc.hasNextLine()) {
			String line = sc.nextLine();
			if(line.startsWith("Container: container_")) {
				// roll new file
				fos.close();
				String[] split = line.split(" ");
				fos = new FileOutputStream(split[1]);
			}
			fos.write(line.getBytes());
			fos.write("\n".getBytes());
		}
		fos.close();
	}
}
