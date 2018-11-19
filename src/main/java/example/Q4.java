package example;

import models.Minmax;
import models.UserMove;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;

import java.sql.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
        Column c4 = triplets.col("s1.userid").alias("userid");
        Column c5 = triplets.col("s1.tpos").alias("tpos");
        Column c6 = triplets.col("s2.tpos").alias("tpos2");
        Column c7 = triplets.col("s1.utctimestamp").alias("utctimestamp");

        Dataset<Row> selectedVertices = triplets.select(c1,c4,c5,c6,c7);

		GraphFrame graphFrame = GraphFrame.apply(selectedVertices, selectedEdges);

        //graphFrame.vertices().printSchema();
        //graphFrame.vertices().show(1000);
        //System.out.println("Total de vertices: " + g.vertices().count());


        //****************************************************************************************************************


        graphFrame.vertices().createOrReplaceTempView("v_max_table");
        Dataset<Row> max_v = graphFrame.sqlContext().sql("select id, userid, utctimestamp, min(tpos) from v_max_table group by id, userid, utctimestamp");

        GraphFrame graphFrame2 = GraphFrame.apply(max_v, selectedEdges);
        graphFrame2.vertices().printSchema();
        graphFrame2.vertices().show(5000);



        Dataset<Row> max_v3 = graphFrame.sqlContext().sql("select id, userid, utctimestamp, max(tpos2) from v_max_table group by id, userid, utctimestamp");

        GraphFrame graphFrame3 = GraphFrame.apply(max_v3, selectedEdges);
        graphFrame3.vertices().printSchema();
        graphFrame3.vertices().show(5000);

        System.out.println("Total de vertices: " + graphFrame2.vertices().count());
        */


        //****************************************************************************************************************

        Map<UserMove, Minmax> map = new HashMap<>();

        super.g.vertices().createOrReplaceTempView("vertex");
        Dataset<Row> q4Vertices = super.g.sqlContext().sql("SELECT id, userid, utctimestamp, tpos from vertex where vertextype = 0");
        Dataset<Row> q4Edges = super.g.edges();

        for (Iterator<Row> it = q4Vertices.toLocalIterator(); it.hasNext(); ) {
            Row each = it.next();
            String userid = (String) each.get(1);
            Date utctimestamp = (Date) each.get(2);
            Long tpos = (Long) each.get(3);

            UserMove userMove = new UserMove(userid, utctimestamp);

            if( !map.containsKey(userMove) ){
                map.put(userMove, new Minmax());
            }

            map.get(userMove).set(tpos);

            //System.out.println(each);
        }

        //GraphFrame g2 = GraphFrame.apply(q4Vertices, q4Edges);
        //g2.vertices().printSchema();
        //g2.vertices().show(1000);

        map.forEach( (k, v) -> {
            System.out.println("**INIT**");
            System.out.println(k.toString() + v.toString());
            System.out.println("**END**");
        });


	}

}