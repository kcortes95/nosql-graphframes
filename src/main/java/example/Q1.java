package example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;


public class Q1 extends GraphRunnable{

	public Q1(GraphFrame g) {
		super(g);
	}

	@Override
	public void run() {

		String q1 = "(s1)-[e11]->(v1); (v1)-[e12]->(c1); (c1)-[e13]->(cs1);";
		String q2 = "(s2)-[e21]->(v2); (v2)-[e22]->(c2); (c2)-[e23]->(cs2);";
		String q3 = "(s3)-[e31]->(v3); (v3)-[e32]->(c3); (c3)-[e33]->(cs3)";
		String q = q1 + q2 + q3;

		String conditionType1 = "s1.vertextype = 0 and v1.vertextype = 2 and c1.vertextype = 4 and cs1.vertextype = 6";
		String conditionType2 = "s2.vertextype = 0 and v2.vertextype = 2 and c2.vertextype = 4 and cs2.vertextype = 6";
		String conditionType3 = "s3.vertextype = 0 and v3.vertextype = 2 and c3.vertextype = 4 and cs3.vertextype = 6";
		String conditionTraj = "cs1.cattype = '\"Home\"' and cs2.cattype = '\"Station\"' and cs3.cattype = '\"Airport\"'";
		String conditionConsecutive = "s2.tpos = s1.tpos + 1 and s3.tpos = s2.tpos + 1";
		String conditionUser = "s1.userid = s2.userid and s1.userid = s3.userid";
		String condition = conditionType1 + " and " + conditionType2 + " and " + conditionType3 + " and " + conditionTraj + " and " + conditionConsecutive + " and " + conditionUser;

		Dataset<Row> triplets = super.g.find(q).filter(condition);
		Dataset<Row> filterededges = super.g.edges();
		Dataset<Row> filteredvertices = triplets.select("s1.id", "s2.id", "s3.id", "v1.id", "v2.id", "v3.id", "c1.id", "c2.id", "c3.id", "cs1.id", "cs2.id", "cs3.id", "s1.userid");


		GraphFrame graphFrame = GraphFrame.apply(filteredvertices, filterededges);
		graphFrame.vertices().printSchema();
		graphFrame.vertices().show(1000);

	}
}