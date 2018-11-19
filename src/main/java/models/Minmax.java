package models;

public class Minmax {

    public long min = 999999999999L;
    public long max = 0;

    public Minmax() {
    }

    public Minmax(long min, long max) {
        this.min = min;
        this.max = max;
    }

    public void set(long num){
        if(num <= min)
            min = num;

        if(num > max)
            max = num;
    }

    @Override
    public String toString() {
        return "Minmax{" +
                "min=" + min +
                ", max=" + max +
                '}';
    }
}
