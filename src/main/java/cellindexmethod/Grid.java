package cellindexmethod;

import models.Venue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public abstract class Grid {

	private static double minLat = 40.55085247;
	private static double maxLat = 40.98833172;
	private static double minLon = -74.27476645;
	private static double maxLon = -73.6838252;

	private Cell[][] cells;
	private double L;
	private int M;

	public Grid(int M, Set<Venue> venues) {
		this.L = Math.max(maxLat - minLat + 0.01, maxLon - minLon + 0.01);
		this.M = M;
		cells = new Cell[M][M];
		for (int i = 0; i < M; i++) {
			for (int j = 0; j < M; j++) {
				cells[i][j] = new Cell();
			}
		}
		insertVenues(venues);
		calculateNeighbours();
	}

	protected abstract void calculateNeighbours();

	private void insertVenues(Set<Venue> venues) {
		for (Venue v : venues) {
			int x = (int) (Math.floor((v.getLatitude() - minLat) / (L / M)));
			int y = (int) (Math.floor((v.getLongitude() - minLon) / (L / M)));
			//Habilitar el codigo de abajo para ver las posiciones reales, y en la grilla de las particulas
			/*
			System.out.println("x: " + p.getPosition().getX() + " - y: " + p.getPosition().getY());
			System.out.println("x: " + x + " - y: " + y);
			*/
			cells[x][y].getVenues().add(v);
		}
	}

	public Cell getCell(int x, int y) {
		return cells[x][y];
	}

	public Cell[][] getGrid() {
		return cells;
	}

	public int getM() {
		return M;
	}

	public double getL() {
		return L;
	}

	public static List<Integer> getGridPos(int m, Double lat, Double lng){
		List<Integer> list = new ArrayList<>();
		double l = Math.max(maxLat - minLat + 0.01, maxLon - minLon + 0.01);

		int x = (int) (Math.floor((lat - minLat) / (l / m)));
		int y = (int) (Math.floor((lng - minLon) / (l / m)));

		list.add(x);
		list.add(y);
		return list;
	}

	public Set<Venue> getVenuesFromPos(double lat, Double lng){
		int x = (int) (Math.floor((lat - minLat) / (L / M)));
		int y = (int) (Math.floor((lng - minLon) / (L / M)));
		return cells[x][y].getVenues();
	}

}
