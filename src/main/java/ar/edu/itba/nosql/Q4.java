package ar.edu.itba.nosql;

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

        long init = System.currentTimeMillis();

        Map<UserMove, Minmax> map = new HashMap<>();

        String q = "(s1)-[e1]->(v1)";
        String condition = "s1.vertextype = 0 and v1.vertextype = 2";

        Dataset<Row> triplets = super.g.find(q).filter(condition);
        Dataset<Row> edges = super.g.edges();

        Column c1 = triplets.col("s1.id").alias("id");
        Column c2 = triplets.col("s1.userid").alias("userid");
        Column c3 = triplets.col("s1.utctimestamp").alias("utctimestamp");
        Column c4 = triplets.col("s1.tpos").alias("tpos");
        Column c5 = triplets.col("v1.venueid").alias("venueid");

        Dataset<Row> selectedVertices = triplets.select(c1,c2,c3,c4,c5);

        GraphFrame graphFrame = GraphFrame.apply(selectedVertices, edges);

        //super.g.vertices().createOrReplaceTempView("vertex");
        //Dataset<Row> q4Vertices = super.g.sqlContext().sql("SELECT id, userid, utctimestamp, tpos, venueid from vertex where vertextype = 0 order by userid, utctimestamp");
        //Dataset<Row> q4Edges = super.g.edges();

        Dataset<Row> q4Vertices = graphFrame.vertices();

        for (Iterator<Row> it = q4Vertices.toLocalIterator(); it.hasNext(); ) {
            Row each = it.next();
            String userid = (String) each.get(1);
            Date utctimestamp = (Date) each.get(2);
            Long tpos = (Long) each.get(3);

            String venueid = (String) each.get(4);

            UserMove userMove = new UserMove(userid, utctimestamp);

            if( !map.containsKey(userMove) ){
                map.put(userMove, new Minmax());
            }

            map.get(userMove).set(tpos, venueid);

        }

        long end = System.currentTimeMillis();

        System.out.println("userid | utctimestamp | tpos_init | venueid_init | tpos_end | venueid_end");
        map.forEach( (k, v) -> {
            System.out.println(k.getUserid() + " | " + k.getUtctimestamp() + " | " + v.getMin() + " | " + v.getVenueidmin() + " | " + v.getMax() + " | " + v.getVenueidmax() );
        });

        System.out.println("****");
        System.out.println("Total de resultados: " + q4Vertices.count());
        System.out.println("Total de tiempo en ejecución: " + (end-init) + " ms");
        System.out.println("****");


	}

}