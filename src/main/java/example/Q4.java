package example;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;

public class Q4 extends GraphRunnable{

	public Q4(GraphFrame g) {
		super(g);
	}

	public void run() {

	    /*
		String q1 = "(s1)-[e1]->(v1); ";
		String q2 = "(s2)-[e2]->(v2)";
		String query = q1 + q2;
		//String query = q1;

		String conditionType1 = "s1.vertextype = 0 and v1.vertextype = 2";
		String conditionType2 = "s2.vertextype = 0 and v2.vertextype = 2";
		String conditionSameUser = "s1.userid = s2.userid";
		String contidionSameDate = "s1.utctimestamp = s2.utctimestamp";
		String conditionTpos = "s2.tpos > s1.tpos";
		String condition = conditionType1 + " and " + conditionType2 + " and " + conditionSameUser + " and " + contidionSameDate + " and " + conditionTpos;
		//String condition = conditionType1;


		Dataset<Row> triplets = super.g.find(query).filter(condition);
		Dataset<Row> selectedEdges = super.g.edges();

        Column c1 = triplets.col("s1.id").alias("id");
        Column c2 = triplets.col("v1.venueid").alias("v");
        Column c3 = triplets.col("v2.venueid").alias("v2");
        Column c4 = triplets.col("s1.userid").alias("userid");
        Column c5 = triplets.col("s1.tpos").alias("tpos");
        Column c6 = triplets.col("s2.tpos").alias("tpos2");
        Column c7 = triplets.col("s1.utctimestamp").alias("utctimestamp");

        Dataset<Row> selectedVertices = triplets.select(c1,c2,c3,c4,c5,c6,c7);
        //Dataset<Row> selectedVertices = triplets.select(c1,c2,c4,c5,c7);

		GraphFrame graphFrame = GraphFrame.apply(selectedVertices, selectedEdges);

        graphFrame.vertices().printSchema();
        //graphFrame.vertices().show(1000);
        //System.out.println("Total de vertices: " + g.vertices().count());
        */
        //****************************************************************************************************************

        super.g.vertices().createOrReplaceTempView("stop_vertex");
        Dataset<Row> stopVertices = super.g.sqlContext().sql("SELECT id, userid, utctimestamp, max(tpos), min(tpos) from stop_vertex where vertextype = 0 group by id, userid, utctimestamp");
        Dataset<Row> stopEdges = super.g.edges();

        GraphFrame g2 = GraphFrame.apply(stopVertices, stopEdges);
        g2.vertices().printSchema();
        g2.vertices().show(1000);

        System.out.println("*****");

        /*
        graphFrame.vertices().createOrReplaceTempView("v_max_table");
        Dataset<Row> max_v = graphFrame.sqlContext().sql("select id, userid, utctimestamp, max(tpos2), min(tpos) from v_max_table group by id, userid, utctimestamp");

        GraphFrame graphFrame2 = GraphFrame.apply(max_v, selectedEdges);
        graphFrame2.vertices().printSchema();
        graphFrame2.vertices().show(2000);

        System.out.println("Total de vertices: " + graphFrame2.vertices().count());
        */
	}


}