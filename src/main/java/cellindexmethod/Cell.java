package cellindexmethod;

import models.Venue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class Cell {
	private Set<Venue> venues;
	private List<Cell> neighbours;

	public Cell() {
		venues = new HashSet<>();
		neighbours = new ArrayList<>();
	}

	public void addNeighbour(Cell c) {
		neighbours.add(c);
	}

	public List<Cell> getNeighbours() {
		return neighbours;
	}

	public Set<Venue> getVenues() {
		return venues;
	}
}
