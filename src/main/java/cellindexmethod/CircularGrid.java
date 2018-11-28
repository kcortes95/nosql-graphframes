package cellindexmethod;

import models.Venue;

import java.util.Set;


public class CircularGrid extends Grid {

	public CircularGrid(int M, Set<Venue> venues) {
		super(M, venues);
	}

	protected void calculateNeighbours() {
		for (int i = 0; i < getM(); i++) {
			for (int j = 0; j < getM(); j++) {
				if (i - 1 >= 0) {
					getGrid()[i][j].addNeighbour(getGrid()[i - 1][j]);
					if (j + 1 < getM()) {
						getGrid()[i][j].addNeighbour(getGrid()[i - 1][j + 1]);
					} else {
						getGrid()[i][j].addNeighbour(getGrid()[i - 1][0]);
					}
				} else {
					getGrid()[i][j].addNeighbour(getGrid()[getM() - 1][j]);
					if (j + 1 < getM()) {
						getGrid()[i][j].addNeighbour(getGrid()[getM() - 1][j + 1]);
						// else es la diagonal y no va
					}
				}
				if (j + 1 < getM()) {
					getGrid()[i][j].addNeighbour(getGrid()[i][j + 1]);
					if (i + 1 < getM()) {
						getGrid()[i][j].addNeighbour(getGrid()[i + 1][j + 1]);
					} else {
						getGrid()[i][j].addNeighbour(getGrid()[0][j + 1]);
					}
				} else {
					getGrid()[i][j].addNeighbour(getGrid()[i][0]);
					if (i + 1 < getM())
						getGrid()[i][j].addNeighbour(getGrid()[i + 1][0]);
					// else es la diagonal y no va
				}
			}
		}
	}
}
