package example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;


public class Q3 extends GraphRunnable{

	public Q3(GraphFrame g) {
		super(g);
	}

	public void run() {

		String q1 = "(s1)-[e1]->(v1);";
		String q2 = "(s2)-[e2]->(v2)";
		String query = q1 + q2;

		String conditionType1 = "s1.vertextype = 0 and v1.vertextype = 2";
		String conditionType2 = "s2.vertextype = 0 and v2.vertextype = 2";
		String conditionSameUser = "s1.userid = s2.userid";
		String conditionSameVenue = "v1.venueid = v2.venueid";
		String contidionSameDate = "s1.utctimestamp = s2.utctimestamp";
		String conditionTpos = "s1.tpos = 1 and s2.tpos > s1.tpos";
		String condition = conditionType1 + " and " + conditionType2 + " and " + conditionSameUser + " and " + conditionSameVenue + " and " + contidionSameDate + " and " + conditionTpos;


		Dataset<Row> triplets = super.g.find(query).filter(condition);
		Dataset<Row> selectedEdges = super.g.edges();
		Dataset<Row> selectedVertices = triplets.select("s1.id", "v1.venueid", "v2.venueid", "s1.userid", "s1.tpos", "s2.tpos");

		//selectedVertices = selectedVertices.filter("s1.tpos = 1 and s2.tpos = 2");

		GraphFrame g = GraphFrame.apply(selectedVertices, selectedEdges);
		g.vertices().printSchema();
		g.vertices().show(1000);
		System.out.println("Total: " + g.vertices().count());
	}


}