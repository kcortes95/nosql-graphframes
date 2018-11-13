package example;

import java.text.ParseException;

import loader.LoadEdges;
import loader.LoadVertices;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import schemas.Edges;
import schemas.Vertex;


public class GraphFramesAppMain {

	public static void main(String[] args) throws ParseException {

		// SparkConf spark = new SparkConf().setAppName("Exercise 8");
		SparkSession sp = SparkSession.builder().appName("Exercise Extra").getOrCreate();
		JavaSparkContext sparkContext = new JavaSparkContext(sp.sparkContext());
		SQLContext sqlContext = new SQLContext(sp);

		// parallelize vertices and edges values
		//Dataset<Row> verticesStop = sqlContext.createDataFrame(sparkContext.parallelize(LoadVertices.LoadVertices()), Vertex.CreateStopsVertex());
		Dataset<Row> edgesStop = sqlContext.createDataFrame(sparkContext.parallelize(LoadEdges.LoadEdges()), Edges.CreateStopStopEdge());

        Dataset<Row> vertices = sqlContext.createDataFrame(sparkContext.parallelize(LoadVertices.LoadVerticesCategory(args[0])), Vertex.CreateCategoryVertex());
        Dataset<Row> verticesCategories = sqlContext.createDataFrame(sparkContext.parallelize(LoadVertices.LoadVerticesCategory2()), Vertex.CreateCategoryVertex2());

        Dataset<Row> newVer = vertices.union(verticesCategories);

		// create the graph
		//GraphFrame myGraph = GraphFrame.apply(verticesStop, edgesStop);
		GraphFrame myGraph = GraphFrame.apply(newVer, edgesStop);

		myGraph.vertices().createOrReplaceTempView("v_table");
		Dataset<Row> newVerticesDF = myGraph.sqlContext().sql("SELECT * from v_table");

		myGraph.vertices().printSchema();
		myGraph.vertices().show();

		//myGraph.edges().createOrReplaceTempView("e_table");
		//Dataset<Row> newEdgesDF = myGraph.sqlContext().sql("SELECT * from e_table");
		//Dataset<Row> newEdgesDF = myGraph.sqlContext().sql("SELECT * from e_table");

		
		//GraphFrame newGraph = GraphFrame.apply(newVerticesDF, newEdgesDF);
		//GraphFrame newGraph = GraphFrame.apply(newVerticesDF, edgesStop);

		//GraphFrame filteredGraph = newGraph.filterVertices("parity=true").filterEdges("year=2010").dropIsolatedVertices();
		
		//in the driver
        /*
		newGraph.vertices().printSchema();
		newGraph.vertices().show();
				
		newGraph.edges().printSchema();
		newGraph.edges().show();
		*/
		//con los datos filtrados
		//filteredGraph.vertices().printSchema();
		//filteredGraph.vertices().show();
		//filteredGraph.edges().printSchema();
		//filteredGraph.edges().show();

		sparkContext.close();

	}


}