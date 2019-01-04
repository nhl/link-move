package com.nhl.link.move.df;

public class IndexPosition {

    private int position;
    private String name;

    public IndexPosition(int position, String name) {
        this.position = position;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public int getPosition() {
        return position;
    }

    public Object read(Object[] row) {
        return row[position];
    }

    public void write(Object[] row, Object val) {
        row[position] = val;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof IndexPosition)) {
            return false;
        }

        IndexPosition ip = (IndexPosition) o;
        return ip.position == this.position && ip.name.equals(this.name);
    }

    @Override
    public String toString() {
        return "IndexPosition [" + position + ":" + name + "]";
    }
}
