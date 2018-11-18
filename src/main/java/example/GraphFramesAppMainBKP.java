package example;

import loader.LoadNewEdges;
import loader.LoadNewVertices;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import schemas.EdgeSchema;
import schemas.VertexSchema;

import java.text.ParseException;


public class GraphFramesAppMainBKP {

	public static void main(String[] args) throws ParseException {

		SparkSession sp = SparkSession.builder().appName("TP NoSQL - GraphFrames").getOrCreate();
		JavaSparkContext sparkContext = new JavaSparkContext(sp.sparkContext());
		SQLContext sqlContext = new SQLContext(sp);

		// parallelize vertices and edges values
		//Dataset<Row> verticesStop = sqlContext.createDataFrame(sparkContext.parallelize(LoadVertices.LoadVertices()), Vertex.CreateStopsVertex());
		//Dataset<Row> edgesStop = sqlContext.createDataFrame(sparkContext.parallelize(LoadEdges.LoadEdges()), Edges.CreateStopStopEdge());

        Dataset<Row> vertices = sqlContext.createDataFrame(sparkContext.parallelize(LoadNewVertices.LoadVertices("","","","")), VertexSchema.CreateVertex());
		Dataset<Row> edges = sqlContext.createDataFrame(sparkContext.parallelize(LoadNewEdges.LoadEdges(LoadNewVertices.getMaps())), EdgeSchema.CreateEdge());

		// create the graph
		//GraphFrame myGraph = GraphFrame.apply(verticesStop, edgesStop);
		GraphFrame myGraph = GraphFrame.apply(vertices, edges);

		myGraph.vertices().createOrReplaceTempView("v_table");
		Dataset<Row> newVerticesDF = myGraph.sqlContext().sql("SELECT * from v_table where vertextype = 0");

        myGraph.edges().createOrReplaceTempView("e_table");
        Dataset<Row> newEdgesDF = myGraph.sqlContext().sql("SELECT * from e_table where edgetype = 3");

		//myGraph.vertices().printSchema();
		//myGraph.vertices().show(1000);

		//GraphFrame g = GraphFrame.apply(vertices, newEdgesDF);
		//g.vertices().printSchema();
		//g.vertices().show(10000);
		//g.edges().printSchema();
		//g.edges().show(1000);

		//myGraph.edges().createOrReplaceTempView("e_table");
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