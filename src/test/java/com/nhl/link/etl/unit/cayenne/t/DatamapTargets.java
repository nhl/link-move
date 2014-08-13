package com.nhl.link.etl.unit.cayenne.t;

import com.nhl.link.etl.unit.cayenne.t.auto._DatamapTargets;

public class DatamapTargets extends _DatamapTargets {

    private static DatamapTargets instance;

    private DatamapTargets() {}

    public static DatamapTargets getInstance() {
        if(instance == null) {
            instance = new DatamapTargets();
        }

        return instance;
    }
}
