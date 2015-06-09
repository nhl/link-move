package com.nhl.link.etl.runtime.path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.exp.ExpressionFactory;
import org.apache.cayenne.exp.parser.ASTDbPath;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbJoin;
import org.apache.cayenne.map.DbRelationship;
import org.apache.cayenne.map.ObjEntity;
import org.apache.cayenne.map.PathComponent;

import com.nhl.link.etl.EtlRuntimeException;

/**
 * @since 1.4
 */
public class PathNormalizer implements IPathNormalizer {

	private ConcurrentMap<String, EntityPathNormalizer> pathCache;

	public PathNormalizer() {
		pathCache = new ConcurrentHashMap<>();
	}

	@Override
	public EntityPathNormalizer normalizer(ObjEntity root) {

		if (root == null) {
			throw new NullPointerException("Null root entity");
		}

		EntityPathNormalizer normalizer = pathCache.get(root.getName());

		if (normalizer == null) {
			EntityPathNormalizer newNormalizer = createNormalizer(root);
			EntityPathNormalizer oldNormalizer = pathCache.putIfAbsent(root.getName(), newNormalizer);
			normalizer = oldNormalizer != null ? oldNormalizer : newNormalizer;
		}

		return normalizer;
	}

	private EntityPathNormalizer createNormalizer(final ObjEntity entity) {

		return new EntityPathNormalizer() {

			ConcurrentMap<String, String> cachedAttributes = new ConcurrentHashMap<>();

			@Override
			public String normalize(String path) {

				if (path == null) {
					throw new NullPointerException("Null path. Entity: " + entity.getName());
				}

				String normalized = cachedAttributes.get(path);
				if (normalized == null) {
					String newPath = doNormalize(path);
					String oldPath = cachedAttributes.putIfAbsent(path, newPath);
					normalized = oldPath != null ? oldPath : newPath;
				}

				return normalized;
			}

			private String doNormalize(String path) {

				Expression dbExp = entity.translateToDbPath(ExpressionFactory.exp(path));

				List<PathComponent<DbAttribute, DbRelationship>> components = new ArrayList<>(2);
				for (PathComponent<DbAttribute, DbRelationship> c : entity.getDbEntity().resolvePath(dbExp,
						Collections.emptyMap())) {
					components.add(c);
				}

				if (components.size() == 0) {
					throw new EtlRuntimeException("Null path. Entity: " + entity.getName());
				}

				if (components.size() > 1) {
					throw new EtlRuntimeException("Nested paths not supported. Path: " + dbExp);
				}

				PathComponent<DbAttribute, DbRelationship> c = components.get(0);

				if (c.getAttribute() != null) {
					return ASTDbPath.DB_PREFIX + c.getAttribute().getName();
				}

				DbRelationship dbRelationship = c.getRelationship();

				List<DbJoin> joins = dbRelationship.getJoins();

				// TODO: logic duplication from DefaultCreateOrUpdateBuilder...
				if (joins.size() > 1) {
					// TODO: support for multi-key to-one relationships
					throw new EtlRuntimeException("Multi-column FKs are not yet supported. Path: " + dbExp);
				} else {
					return ASTDbPath.DB_PREFIX + joins.get(0).getSourceName();
				}
			}
		};
	}
}
