package ar.edu.itba.nosql;

import cellindexmethod.Grid;
import loader.LoadNewVertices;
import models.Venue;
import org.graphframes.GraphFrame;

import java.util.Set;


public class Q2 extends GraphRunnable{

	public Q2(GraphFrame g) {
		super(g);
	}

	@Override
	public void run() {

		long init = System.currentTimeMillis();

		Grid g = LoadNewVertices.getGrid();
		double lat = 40.72987459;
		double lng = -73.98918676;
		Set<Venue> venues = g.getVenuesFromPos(lat,lng);

		venues.forEach( v -> {
			System.out.println(v.toString() + " - distance " + v.distance(lat, lng));
		});

		long end = System.currentTimeMillis();

		System.out.println("****");
		System.out.println("Total de resultados: " + venues.size());
		System.out.println("Total de tiempo en ejecución: " + (end-init) + " ms");
		System.out.println("****");

	}
}