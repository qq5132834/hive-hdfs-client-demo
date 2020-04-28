package com.sisc.hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class TestHDFS {

	public static Configuration conf = null;
	public static FileSystem fs = null;
	
	public void before() throws Exception{
		conf = new Configuration(true);
//		fs = FileSystem.get(conf);
//	<property>
//        <name>fs.defaultFS</name>
//        <value>hdfs://192.168.133.139:9000</value>
//    </property>
		
		fs = FileSystem.get(URI.create("hdfs://192.168.133.139:9000"), conf, "root");
		
		System.out.println(fs.getClass().getName());
	}
	
	
	public void mkdir() throws Exception{
		Path path = new Path("/sics");
		if(fs.exists(path)){
			fs.delete(path);
		}
		boolean st = fs.mkdirs(path);

		System.out.println(st);
	}
	
	
	public void upload() throws Exception{
		BufferedInputStream input = new BufferedInputStream(new FileInputStream(new File("D:/development-java/gitlib/seiois-dev/hdfs-server/data/hello.txt")));
		Path outfile = new Path("/sics/out.txt");
		FSDataOutputStream output = fs.create(outfile);
//		IOUtils.copyBytes(input,output,true);
		IOUtils.copyBytes(input, output, conf, true);
	}
	
	public void download(){
		
	}
	
	public static void main(String[] args) throws Exception{
		
		TestHDFS t = new TestHDFS();
		t.before();
		t.mkdir();
		t.upload();
		
		
		t.after();
	}
	
	
	
	public void after() throws Exception{
		fs.close();
	}
	
}
