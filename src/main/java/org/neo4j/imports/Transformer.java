package org.neo4j.imports;

import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * @author mh
 * @since 01.03.15
 */
public class Transformer {

    private static final Object[] NO_PROPS = new Object[0];

    public void stream(TableInfo table, Rules rules, final ResultSet rs, BlockingQueue<InputNode> nodes, BlockingQueue<InputRelationship> rels) throws SQLException, InterruptedException {
        if (rules.isNode(table)) {
            streamNodes(table, rules, rs, nodes, rels);
        } else {
            streamRels(table, rules, rs, rels);
        }
    }

    public void streamNodes(TableInfo table, Rules rules, final ResultSet rs, BlockingQueue<InputNode> nodes, BlockingQueue<InputRelationship> rels) throws SQLException, InterruptedException {
        Group group = new Group.Adapter(table.index, table.table);
        String[] labels = rules.labelsFor(table);
        Object[] props = prepareProps(table, rules);
        RelInfo[] relInfos = rules.relsFor(table);

        while (rs.next()) {
            Object id = rules.transformPk(rs.getObject(table.pk));
            nodes.put(new InputNode(group, id, extractProps(table, rs, props), null, labels, null));

            for (RelInfo relInfo : relInfos) {
                Object fkId = rules.transformPk(rs.getObject(relInfo.field));
                InputRelationship relationship = new InputRelationship(NO_PROPS, null, group, id, relInfo.group, fkId, relInfo.type, null);
                rels.put(relationship);
            }
        }
        ;
    }

    private Object[] extractProps(TableInfo table, ResultSet rs, Object[] props) throws SQLException {
        if (table.fieldCount() == 0) return NO_PROPS;

        int length = props.length / 2;
        Object[] newProps = Arrays.copyOf(props, length * 2);
        for (int i = 0; i < length; i++) {
            newProps[1 + i*2] = rs.getObject(table.fields[i]);
        }
        return newProps;
    }

    private Object[] prepareProps(TableInfo table, Rules rules) {
        if (table.fieldCount()==0) return null;
        String[] propNames = rules.propertyNamesFor(table);
        Object[] props = new Object[propNames.length * 2];
        for (int i = 0; i < propNames.length; i++) {
            props[i * 2] = propNames[i];
        }
        return props;
    }

    public void streamRels(TableInfo table, Rules rules, final ResultSet rs, BlockingQueue<InputRelationship> rels) throws SQLException, InterruptedException {
        String relType = rules.relTypeFor(table);
        Object[] props = prepareProps(table,rules);
        RelInfo[] relInfos = rules.relsFor(table);

        while (rs.next()) {
            rels.put(new InputRelationship(extractProps(table,rs,props), null,
                    relInfos[0].group, rules.transformPk(rs.getObject(relInfos[0].field)),
                    relInfos[1].group, rules.transformPk(rs.getObject(relInfos[1].field)),
                    relType, null));
        }
    }
}
