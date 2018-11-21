package ar.edu.itba.nosql;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;


public class Q2 extends GraphRunnable {

	public Q2(GraphFrame g) {
		super(g);
	}

	public void run() {

		long init = System.currentTimeMillis();

		String q1 = "(s1)-[e11]->(v1); (v1)-[e12]->(c1); (c1)-[e13]->(cs1);";
		String q2 = "(s2)-[e21]->(v2); (v2)-[e22]->(c2); (c2)-[e23]->(cs2)";
		String q = q1 + q2;

		String conditionType1 = "s1.vertextype = 0 and v1.vertextype = 2 and c1.vertextype = 4 and cs1.vertextype = 6";
		String conditionType2 = "s2.vertextype = 0 and v2.vertextype = 2 and c2.vertextype = 4 and cs2.vertextype = 6";
		String conditionUser = "s1.userid = s2.userid";
		String conditionDate = "s1.utctimestamp = s2.utctimestamp and s1.tpos < s2.tpos";
		String conditionVisited = "cs1.cattype = 'Home' and cs2.cattype = 'Airport'";
		//String conditionVisited = "cs1.cattype = 'Home'";
		String condition = conditionType1 + " and " + conditionType2 + " and " + conditionUser + " and " + conditionDate + " and " + conditionVisited ;

		Dataset<Row> triplets = super.g.find(q).filter(condition);
		Dataset<Row> filterededges = super.g.edges();

		Column c1 = triplets.col("s1.id").as("id");
		Column c2 = triplets.col("s1.userid").as("userid");
		Column c3 = triplets.col("cs1.cattype").as("from");
		Column c4 = triplets.col("cs2.cattype").as("to");
		Column c5 = triplets.col("s1.tpos").as("tpos1");
		Column c6 = triplets.col("s2.tpos").as("tpos2");

		//Dataset<Row> filteredvertices = triplets.select("s1.id",  "s1.userid", "cs1.cattype", "cs2.cattype");
		Dataset<Row> filteredvertices = triplets.select(c1,c2,c3,c4,c5,c6);

		GraphFrame graphFrame = GraphFrame.apply(filteredvertices, filterededges);
		//graphFrame.vertices().printSchema();
		//graphFrame.vertices().show(1000);

        graphFrame.vertices().createOrReplaceTempView("v_table");
        Dataset<Row> newVertices = graphFrame.sqlContext().sql("SELECT id, userid, from || '>' || to, tpos1, tpos2, tpos2 - tpos1 from v_table order by userid asc");

        GraphFrame graphFrame2 = GraphFrame.apply(newVertices, filterededges);
        graphFrame2.vertices().printSchema();

		long end = System.currentTimeMillis();

        long tot = graphFrame2.vertices().count();

        graphFrame2.vertices().show((int) tot, false);

		System.out.println("****");
		System.out.println("Total de resultados: " + tot);
		System.out.println("Total de tiempo en ejecución: " + (end-init) + " ms");
		System.out.println("****");





	}


}