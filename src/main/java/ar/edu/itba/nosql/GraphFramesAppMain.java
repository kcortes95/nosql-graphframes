package ar.edu.itba.nosql;

import java.text.ParseException;

import cellindexmethod.Grid;
import loader.LoadNewEdges;
import loader.LoadNewVertices;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;
import schemas.EdgeSchema;
import schemas.VertexSchema;


public class GraphFramesAppMain {

	public static void main(String[] args) throws ParseException {

		SparkSession sp = SparkSession.builder().appName("TP FINAL NoSQL - GraphFrames").getOrCreate();
		JavaSparkContext sparkContext = new JavaSparkContext(sp.sparkContext());
		SQLContext sqlContext = new SQLContext(sp);

		double maxDistance = Double.parseDouble(args[0]);
		String pathCatType = args[1];
		String pathVenueCategory = args[2];
		String pathVenues = args[3];


		long start = System.currentTimeMillis();
		Dataset<Row> vertices = sqlContext.createDataFrame(sparkContext.parallelize(
				LoadNewVertices.LoadVertices(pathCatType, pathVenueCategory, pathVenues)), VertexSchema.CreateVertex());
		Dataset<Row> edges = sqlContext.createDataFrame(sparkContext.parallelize(
				LoadNewEdges.LoadEdges(maxDistance)), EdgeSchema.CreateEdge());

		GraphFrame myGraph = GraphFrame.apply(vertices, edges);

		System.out.println("Time taken: " + (System.currentTimeMillis() - start) / 1000.0);


		GraphRunnable userQuery = new Q1(myGraph);
		GraphRunnable userQuery2 = new Q2(myGraph);

		userQuery.run();
		System.out.println("********COMIENZA Q2********");
		userQuery2.run();

		sparkContext.close();

	}


}