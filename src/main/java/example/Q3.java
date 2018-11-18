package example;

import org.apache.spark.sql.Column;
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

        Column c1 = triplets.col("s1.id").alias("id");
        Column c2 = triplets.col("v1.venueid").alias("v1");
        Column c3 = triplets.col("v2.venueid").alias("v2");
        Column c4 = triplets.col("s1.userid").alias("userid");
        Column c5 = triplets.col("s1.tpos").alias("tpos1");
        Column c6 = triplets.col("s2.tpos").alias("tpos2");

        Dataset<Row> selectedVertices = triplets.select(c1,c2,c3,c4,c5,c6);

		GraphFrame graphFrame = GraphFrame.apply(selectedVertices, selectedEdges);

        System.out.println("Resultado...");
        graphFrame.vertices().printSchema();
        graphFrame.vertices().show(1000);
		System.out.println("Total: " + graphFrame.vertices().count());

	}


}