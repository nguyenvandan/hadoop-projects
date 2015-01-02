package com.lab.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class IndexMapper extends
		Mapper<LongWritable, Text, NullWritable, NullWritable> {
	
	private ConcurrentUpdateSolrServer server = null;
	private SolrInputDocument thisDoc = new SolrInputDocument();
	private String fileName;
	private StringTokenizer st = null;
	private int lineCounter = 0;

	@Override
	public void setup(Context context) {
		String url = "http://quickstart.cloudera:8983/solr/log_collection";
		fileName = ((FileSplit) context.getInputSplit()).getPath()
				.toString();
		server = new ConcurrentUpdateSolrServer(url, 100, 5);
	}

	@Override
	public void map(LongWritable key, Text val, Context context)
			throws IOException, InterruptedException {

		st = new StringTokenizer(val.toString());
		lineCounter = 0;
		while (st.hasMoreTokens()) {
			thisDoc = new SolrInputDocument();	
			thisDoc.addField("id", fileName + " " + key.toString() + " " + lineCounter++);
			thisDoc.addField("text",  st.nextToken());
			try {
				server.add(thisDoc);
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		try {
			server.commit();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
	}
}