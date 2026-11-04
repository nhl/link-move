package org.apache.cayenne.access.translator.select;

import org.apache.cayenne.CayenneRuntimeException;
import org.apache.cayenne.access.jdbc.PSParameter;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LmDeleteTranslatorTest {

    private static PSParameter<?> param(Object value, int position, int type, int scale) {
        return new PSParameter<>(value, position, type, scale, null, null);
    }

    @Test
    public void toTemplate() {
        List<PSParameter<?>> bindings = List.of(
                param("a%", 1, Types.VARCHAR, -1),
                param(new BigDecimal("1.50"), 2, Types.DECIMAL, 2));
        Map<String, Object> params = new HashMap<>();

        String template = LmDeleteTranslator.toTemplate(
                "DELETE FROM \"t\" WHERE \"name\" LIKE ? AND \"price\" > CAST(? AS DECIMAL(10, 2))", bindings, params);

        assertEquals("DELETE FROM \"t\" WHERE \"name\" LIKE #bind($p0 'VARCHAR')"
                + " AND \"price\" > CAST(#bind($p1 'DECIMAL' 2) AS DECIMAL(10, 2))", template);
        assertEquals(Map.of("p0", "a%", "p1", new BigDecimal("1.50")), params);
    }

    @Test
    public void toTemplate_QuotedPlaceholders() {
        List<PSParameter<?>> bindings = List.of(param(5, 1, Types.INTEGER, -1));
        Map<String, Object> params = new HashMap<>();

        String template = LmDeleteTranslator.toTemplate(
                "DELETE FROM \"t?\" WHERE `x?` = 'a?b' AND [y?] = ?", bindings, params);

        assertEquals("DELETE FROM \"t?\" WHERE `x?` = 'a?b' AND [y?] = #bind($p0 'INTEGER')", template);
        assertEquals(Map.of("p0", 5), params);
    }

    @Test
    public void toTemplate_BindingsMismatch() {
        assertThrows(CayenneRuntimeException.class,
                () -> LmDeleteTranslator.toTemplate("DELETE FROM t WHERE a = ? AND b = ?",
                        List.of(param(1, 1, Types.INTEGER, -1)), new HashMap<>()));

        assertThrows(CayenneRuntimeException.class,
                () -> LmDeleteTranslator.toTemplate("DELETE FROM t WHERE a = ?",
                        List.of(param(1, 1, Types.INTEGER, -1), param(2, 2, Types.INTEGER, -1)), new HashMap<>()));
    }
}
