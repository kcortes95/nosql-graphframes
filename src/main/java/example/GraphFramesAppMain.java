package example;

import java.text.ParseException;

import loader.LoadNewEdges;
import loader.LoadNewVertices;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;
import schemas.EdgeSchema;
import schemas.VertexSchema;


public class GraphFramesAppMain {

	public static void main(String[] args) throws ParseException {

		SparkSession sp = SparkSession.builder().appName("TP NoSQL - GraphFrames").getOrCreate();
		JavaSparkContext sparkContext = new JavaSparkContext(sp.sparkContext());
		SQLContext sqlContext = new SQLContext(sp);

		Dataset<Row> vertices = sqlContext.createDataFrame(sparkContext.parallelize(LoadNewVertices.LoadVertices("","","","")), VertexSchema.CreateVertex());
		Dataset<Row> edges = sqlContext.createDataFrame(sparkContext.parallelize(LoadNewEdges.LoadEdges(LoadNewVertices.getMaps())), EdgeSchema.CreateEdge());

		GraphFrame myGraph = GraphFrame.apply(vertices, edges);

		GraphRunnable userQuery = new Q1(myGraph);
		Integer userOption = Integer.parseInt(args[0]);

		switch (userOption){
			case 1:
				userQuery = new Q1(myGraph);
				break;
			case 2:
				userQuery = new Q2(myGraph);
				break;
			case 3:
				userQuery = new Q3(myGraph);
				break;
			case 4:
				userQuery = new Q4(myGraph);
				break;
			default:
				userQuery = new Q3(myGraph);
				break;
		}

		userQuery.run();
		sparkContext.close();

	}


}