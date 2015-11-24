package com.density;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DensityReducer extends Reducer<Text, Text, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String densityData = null;
		String[] dFields = null;
		double population = 0;
		double houses = 0;
		double land = 0;
		
		for(Text value : values){
			densityData = value.toString();
			dFields = densityData.split("\t", -1);
			population += Double.parseDouble(dFields[0]);
			houses += Double.parseDouble(dFields[1]);
			land += Double.parseDouble(dFields[2]);
		}
		
		context.write(key, new DoubleWritable((houses * land)/(1000000 * population)));
	}
}