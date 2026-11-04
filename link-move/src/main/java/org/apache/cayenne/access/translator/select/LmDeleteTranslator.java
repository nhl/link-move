package org.apache.cayenne.access.translator.select;

import org.apache.cayenne.CayenneRuntimeException;
import org.apache.cayenne.access.jdbc.PSParameter;
import org.apache.cayenne.access.sqlbuilder.DefaultSQLAppendable;
import org.apache.cayenne.access.sqlbuilder.SQLBuilder;
import org.apache.cayenne.access.sqlbuilder.SQLGenerationVisitor;
import org.apache.cayenne.access.sqlbuilder.sqltree.Node;
import org.apache.cayenne.dba.DbAdapter;
import org.apache.cayenne.dba.TypesMapping;
import org.apache.cayenne.map.DbEntity;
import org.apache.cayenne.map.EntityResolver;
import org.apache.cayenne.query.ObjectSelect;
import org.apache.cayenne.query.SQLTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Translates the "where" clause of an {@link ObjectSelect} into a {@link SQLTemplate} that bulk-deletes the matching
 * rows, using Cayenne's own SQL translator for the qualifier. Cayenne 5.0 has no bulk delete query (EJBQL is gone),
 * and its qualifier translator is package-private, hence this class lives in the Cayenne package. This is an internal
 * LinkMove class and not a part of its public API.
 *
 * @since 4.0.0
 */
// TODO: get rid of this hack with "org.apache.cayenne.access.translator.select" package
//   Future version of ObjectSelect should support deletes!!
public class LmDeleteTranslator {

    /**
     * Returns a non-selecting SQLTemplate with a DELETE statement matching the query "where" clause, including the
     * implicit entity inheritance qualifier. Only single-table qualifiers are supported; a qualifier with relationship
     * paths (i.e. one that requires joins) results in an exception.
     */
    public static SQLTemplate deleteTemplate(ObjectSelect<?> select, DbAdapter adapter, EntityResolver resolver) {

        SelectTranslatorContext context = new SelectTranslatorContext(select, adapter, resolver, null);
        new QualifierTranslationStage().perform(context);

        if (!context.isSingleTableSQL()) {
            throw new CayenneRuntimeException(
                    "Can't translate a qualifier with joins to a single-table DELETE: %s", select.getWhere());
        }

        DbEntity table = context.getRootDbEntity();
        Node qualifier = context.getQualifierNode();

        Node delete = SQLBuilder.delete(table)
                .where(qualifier != null ? SQLBuilder.node(qualifier) : null)
                .build();
        delete = adapter.getSqlTreeProcessor().process(delete);

        DefaultSQLAppendable buffer = new DefaultSQLAppendable(adapter.getQuotingStrategy(table));
        delete.visit(new SQLGenerationVisitor(buffer, context));

        List<PSParameter<?>> bindings = context.getBindings();
        Map<String, Object> params = new HashMap<>(bindings.size() * 2);
        String sql = toTemplate(buffer.getSql(), bindings, params);

        SQLTemplate template = new SQLTemplate(table.getDataMap(), sql, false);
        template.setParams(params);
        return template;
    }

    /**
     * Replaces the "?" JDBC placeholders in the generated SQL with "#bind(..)" directives, in the order of the
     * bindings, collecting the bound values in the "params" map. Placeholders inside quoted literals and identifiers
     * are left alone.
     */
    static String toTemplate(String sql, List<PSParameter<?>> bindings, Map<String, Object> params) {

        StringBuilder template = new StringBuilder(sql.length() + bindings.size() * 24);
        int paramIndex = 0;
        char quote = 0;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
                template.append(c);
            } else if (c == '\'' || c == '"' || c == '`') {
                quote = c;
                template.append(c);
            } else if (c == '[') {
                quote = ']';
                template.append(c);
            } else if (c == '?') {
                if (paramIndex >= bindings.size()) {
                    throw new CayenneRuntimeException("More placeholders than bindings in the generated SQL: %s", sql);
                }

                PSParameter<?> binding = bindings.get(paramIndex);
                String name = "p" + paramIndex++;
                params.put(name, binding.value());

                template.append("#bind($").append(name)
                        .append(" '").append(TypesMapping.getSqlNameByType(binding.psType())).append("'");
                if (binding.psScale() >= 0) {
                    template.append(' ').append(binding.psScale());
                }
                template.append(')');
            } else {
                template.append(c);
            }
        }

        if (paramIndex != bindings.size()) {
            throw new CayenneRuntimeException("Fewer placeholders than bindings in the generated SQL: %s", sql);
        }

        return template.toString();
    }
}
