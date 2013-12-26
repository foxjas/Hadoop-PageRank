package indiana.cgl.hadoop.pagerank;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator; // new import 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
 
public class PageRankMap extends Mapper<LongWritable, Text, LongWritable, Text> {

// each map task handles one line within an adjacency matrix file
// key: file offset
// value: <sourceUrl PageRank#targetUrls>
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			int numUrls = context.getConfiguration().getInt("numUrls",1);
			String line = value.toString();
			// instance an object that records the information for one webpage
			RankRecord rrd = new RankRecord(line);
			int sourceUrl, targetUrl;
			// double rankValueOfSrcUrl;
			
			StringBuilder mapOutput = new StringBuilder();
			
			if (rrd.targetUrlsList.size()<=0){
				// there is no out degree for this webpage; 
				// scatter its rank value to all other urls
				double rankValuePerUrl = rrd.rankValue/(double)numUrls;
				for (int i=0;i<numUrls;i++){
				context.write(new LongWritable(i), new Text(String.valueOf(rankValuePerUrl)));
				}
			} else {
			/*Write your code here*/
				/*Write your code here*/
				double rankValuePerOutUrl = rrd.rankValue/(double)rrd.targetUrlsList.size();
				mapOutput.append(rankValuePerOutUrl);
				Iterator<Integer> targetUrlsIterator = rrd.targetUrlsList.iterator();
				while (targetUrlsIterator.hasNext()) {
					int nextUrl = targetUrlsIterator.next();
					mapOutput.append("#" + nextUrl);
				}
			} //for
			context.write(new LongWritable(rrd.sourceUrl), new Text(mapOutput.toString()));
		} // end map

}
