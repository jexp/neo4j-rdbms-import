package org.neo4j.imports;

import org.neo4j.unsafe.impl.batchimport.input.Group;

/**
* @author mh
* @since 01.03.15
*/
public class RelInfo {
    public final Group group;
    public final String type;
    public final String field;

    public RelInfo(Group group, String type, String field) {
        this.group = group;
        this.type = type;
        this.field = field;
    }
}
