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


public class GraphFramesAppMainALL {

	public static void main(String[] args) throws ParseException {

		SparkSession sp = SparkSession.builder().appName("TP NoSQL - GraphFrames").getOrCreate();
		JavaSparkContext sparkContext = new JavaSparkContext(sp.sparkContext());
		SQLContext sqlContext = new SQLContext(sp);

        Dataset<Row> vertices = sqlContext.createDataFrame(sparkContext.parallelize(LoadNewVertices.LoadVertices("","","","")), VertexSchema.CreateVertex());
		Dataset<Row> edges = sqlContext.createDataFrame(sparkContext.parallelize(LoadNewEdges.LoadEdges(LoadNewVertices.getMaps())), EdgeSchema.CreateEdge());

		GraphFrame myGraph = GraphFrame.apply(vertices, edges);

		myGraph.vertices().createOrReplaceTempView("v_table");
		Dataset<Row> newVertices = myGraph.sqlContext().sql("SELECT * from v_table where vertextype = 0");

		myGraph.edges().createOrReplaceTempView("e_table");
		Dataset<Row> newEdges = myGraph.sqlContext().sql("select * from e_table where edgetype = 1");

		GraphFrame g = GraphFrame.apply(newVertices, newEdges);
		g.vertices().printSchema();
		g.vertices().show(1000);

		sparkContext.close();

	}


}