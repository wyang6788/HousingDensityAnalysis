package com.density;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The DensityReducer class processes relevant data for four regions of the US
 */
public class DensityReducer extends Reducer<Text, Text, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String densityData = null;
		String[] dFields = null;
		double population = 0;
		double houses = 0;
		double land = 0;
		double count = 0;
		double PWHD = 0;
		
		for(Text value : values){
			densityData = value.toString();
			dFields = densityData.split("\t", -1);
			population = Double.parseDouble(dFields[0]);
			houses = Double.parseDouble(dFields[1]);
			land = Double.parseDouble(dFields[2]);
			if(houses != 0 && population != 0 && land != 0) PWHD += (houses * land)/(population * 1000000);
			count++; 
		}
		context.write(key, new DoubleWritable(PWHD/count));
	}
}