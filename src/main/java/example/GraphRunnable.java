package example;


import org.graphframes.GraphFrame;

public abstract class GraphRunnable {

    GraphFrame g = null;

    public GraphRunnable(GraphFrame g) {
        this.g = g;
    }

    public abstract void run();

}
