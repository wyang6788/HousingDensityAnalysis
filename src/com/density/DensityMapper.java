package com.density;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The DensityMapper class maps a city's relevant data to the NW, NE, SW, and SE regions of the US
 */
public class DensityMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split("\t", -1);
		
		if(fields[7].equals("INTPTLAT")) return;
		
		String densityData = fields[1] + "\t" + fields[2] + "\t" + fields[3];
		double latitude = Double.parseDouble(fields[7]);
		double longitude = Double.parseDouble(fields[8]);
		String position = null;
		
		if(latitude > 39.833333 && longitude < -98.583333){
			position = "northwest";
		}
		else if(latitude > 39.833333 && longitude > -98.583333){
			position = "northeast";
		}
		else if(latitude < 39.833333 && longitude < -98.583333){
			position = "southwest";
		}
		else if(latitude < 39.833333 && longitude > -98.583333){
			position = "southeast";
		}
		
		if(position != null) context.write(new Text(position), new Text(densityData));
	}
}