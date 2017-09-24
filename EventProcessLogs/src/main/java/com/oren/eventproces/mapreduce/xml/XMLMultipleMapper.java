package com.oren.eventproces.mapreduce.xml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.oren.eventproces.utils.EventProcessLog;

	
public class XMLMultipleMapper extends Mapper<LongWritable, Text, Text, EventProcessLog> {

	/*
	<kalyan>
	<record>
		<dt>2013-11-24T19:08:21</dt>
		<status>SUCCESS</status>
		<ip>189.89.146.110</ip>
		<country>FR</country>
	</record>
	</kalyan>*/
	
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, EventProcessLog>.Context context)
			throws IOException, InterruptedException {

	
		try {
			InputStream is = new ByteArrayInputStream(value.toString().getBytes());
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(is);
			doc.getDocumentElement().normalize();
			NodeList nList = doc.getElementsByTagName("record");
			for (int temp = 0; temp < nList.getLength(); temp++) {
				Node nNode = nList.item(temp);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					String date = eElement.getElementsByTagName("dt").item(0).getTextContent();
					String status = eElement.getElementsByTagName("status").item(0).getTextContent();
					String ip = eElement.getElementsByTagName("ip").item(0).getTextContent();
					String country = eElement.getElementsByTagName("country").item(0).getTextContent();
					
					
					
					EventProcessLog eventProcess = new EventProcessLog();
					eventProcess.setProcesstimestamp(date);
					eventProcess.setIp(ip);
					eventProcess.setCountry(country);
					eventProcess.setStatus(status);
					context.write(new Text(country), eventProcess);
				}
			}
		} catch (Exception e) {

		}
	}
		
	
	}
	
	


