package example;

import loader.LoadNewVertices;
import models.Stop;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
        //String contidionSameDate = "true";
        String conditionTpos = "s2.tpos > s1.tpos";
        //String conditionTpos = "true";
        String condition = conditionType1 + " and " + conditionType2 + " and " + conditionSameUser + " and " + conditionSameVenue + " and " + contidionSameDate + " and " + conditionTpos;

		Dataset<Row> triplets = super.g.find(query).filter(condition);
		Dataset<Row> selectedEdges = super.g.edges();

        Column c1 = triplets.col("s1.id").alias("id");
        Column c2 = triplets.col("v1.venueid").alias("v1");
        Column c3 = triplets.col("v2.venueid").alias("v2");
        Column c4 = triplets.col("s1.userid").alias("userid");
        Column c5 = triplets.col("s1.tpos").alias("tpos1");
        Column c6 = triplets.col("s2.tpos").alias("tpos2");
        Column c7 = triplets.col("s2.utctimestamp").alias("utctimestamp");

        Dataset<Row> selectedVertices = triplets.select(c1,c2,c3,c4,c5,c6,c7);

		GraphFrame graphFrame = GraphFrame.apply(selectedVertices, selectedEdges);

		Dataset<Row> vs = graphFrame.vertices();

        System.out.println("userid | date | path | tpos1 | tpos2");

        for (Iterator<Row> it = vs.toLocalIterator(); it.hasNext(); ) {
            Row each = it.next();
            String userid = (String) each.get(3);
            Long tpos1 = (Long) each.get(4);
            Long tpos2 = (Long) each.get(5);
            Date timestamp = (Date) each.get(6);

            List<String> venuesIdRelated = new ArrayList<>();

            for(Long i = tpos1; i <= tpos2; i++){
                Stop stop = new Stop(userid, "", i.toString());
                venuesIdRelated.add(LoadNewVertices.getMapStopsVenue().get(stop));
            }

            System.out.println(userid + " | " + timestamp.toString() + " | " + venuesIdRelated.toString() + " | " + tpos1 + " | " + tpos2);

        }

        System.out.println("Resultado...");
        //graphFrame.vertices().printSchema();
        //graphFrame.vertices().show(1000);
		System.out.println("Total: " + graphFrame.vertices().count());

	}


}