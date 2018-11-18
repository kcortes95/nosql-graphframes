package example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;


public class Q2 extends GraphRunnable {

	public Q2(GraphFrame g) {
		super(g);
	}

	public void run() {

		String q1 = "(s1)-[e11]->(v1); (v1)-[e12]->(c1); (c1)-[e13]->(cs1);";
		String q2 = "(s2)-[e21]->(v2); (v2)-[e22]->(c2); (c2)-[e23]->(cs2)";
		String q = q1 + q2;

		String conditionType1 = "s1.vertextype = 0 and v1.vertextype = 2 and c1.vertextype = 4 and cs1.vertextype = 6";
		String conditionType2 = "s2.vertextype = 0 and v2.vertextype = 2 and c2.vertextype = 4 and cs2.vertextype = 6";
		String conditionUser = "s1.userid = s2.userid";
		String conditionDate = "s1.utctimestamp = s2.utctimestamp and s1.tpos < s2.tpos";
		String conditionVisited = "cs1.cattype = '\"Home\"' and cs2.cattype = '\"Airport\"'";
		String condition = conditionType1 + " and " + conditionType2 + " and " + conditionUser + " and " + conditionDate + " and " + conditionVisited ;

		Dataset<Row> triplets = super.g.find(q).filter(condition);
		Dataset<Row> filterededges = super.g.edges();
		Dataset<Row> filteredvertices = triplets.select("s1.id", "s2.id", "v1.id", "v2.id", "c1.id", "c2.id", "cs1.id", "cs2.id", "s1.userid");


		GraphFrame graphFrame = GraphFrame.apply(filteredvertices, filterededges);
		graphFrame.vertices().printSchema();
		graphFrame.vertices().show(1000);

	}


}