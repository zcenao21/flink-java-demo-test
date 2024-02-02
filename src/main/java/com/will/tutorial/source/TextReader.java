package com.will.tutorial.source;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSource.FileSourceBuilder;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TextReader {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		String inputFile = "/Users/will/projects/flink-java-demo-test/src/main/resources/input/word.txt";

		FileSource<String> fileSource =
				FileSource.forRecordStreamFormat(
						new TextLineInputFormat(),
						new Path(inputFile)).build();

		env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "textReader")
				.print();
		env.execute();
	}
}
