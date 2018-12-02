package ar.edu.itba.nosql;

import loader.LoadNewVertices;
import models.Venue;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;
import schemas.EdgeType;

import java.util.Iterator;


public class Q1 extends GraphRunnable{

	public Q1(GraphFrame g) {
		super(g);
	}

	@Override
	public void run() {

		Venue v = LoadNewVertices.getIdToVenue().get("40be6a00f964a520c5001fe3");

		long init = System.currentTimeMillis();

		String q = "(src)-[e]->(to)";

		String condition = String.format("src.venueid = '40be6a00f964a520c5001fe3' and src.vertextype = %d and to.vertextype = %d and e.edgetype = %d", 2, 2, EdgeType.VENUE_TO_VENUE.getValue());

		Dataset<Row> triplets = super.g.find(q).filter(condition);

		Column c1 = triplets.col("src.id").as("id");
		Column c2 = triplets.col("to.venueid").as("to_id");
		Column c3 = triplets.col("e.edgetype").as("edgetype");
		Column c4 = triplets.col("src.vertextype").as("src.vtype");
		Column c5 = triplets.col("to.vertextype").as("to.vtype");

		Column c6 = triplets.col("e.src").as("src");
		Column c7 = triplets.col("e.dst").as("dst");

		Dataset<Row> filterededges = triplets.select(c6,c7);
		Dataset<Row> filteredvertices = triplets.select(c1, c2, c3, c4, c5);

		GraphFrame graphFrame = GraphFrame.apply(filteredvertices, filterededges);

		graphFrame.vertices().printSchema();
		graphFrame.vertices().show(1000, false);

		for (Iterator<Row> it = graphFrame.vertices().toLocalIterator(); it.hasNext();) {
			Row each = it.next();
			String venueid = (String) each.get(1);
			double distance = v.distance(LoadNewVertices.getIdToVenue().get(venueid));
			System.out.println("Distance to " + venueid + " = " + distance);
		}

		long end = System.currentTimeMillis();


		System.out.println("****");
		System.out.println("Total de resultados: " + graphFrame.vertices().count());
		System.out.println("Total de tiempo en ejecución: " + (end-init) + " ms");
		System.out.println("****");

	}
}