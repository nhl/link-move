package com.nhl.link.move.unit.cayenne.t;

import com.nhl.link.move.unit.cayenne.t.auto._Etl7t;

public class Etl7t extends _Etl7t {

    private static final long serialVersionUID = 1L;

    public void setFullName(String fullName) {

        if (fullName == null) {
            return;
        }

        String[] parts = fullName.split(",\\s+");

        if (parts.length > 0) {
            super.setLastName(parts[0]);

            if (parts.length > 1) {
                super.setFirstName(parts[1]);
            }
        }
    }

    @Override
    public void setSex(String sex) {
        // being politically correct
        super.setSex(null);
    }
}
