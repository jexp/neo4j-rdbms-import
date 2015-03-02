package org.neo4j.imports;

import org.neo4j.unsafe.impl.batchimport.input.Group;

import java.util.Arrays;

/**
* @author mh
* @since 01.03.15
*/
public class RelInfo {
    public final Group group;
    public final String type;
    public final String[] fields;
    public final String fieldsString;

    public RelInfo(Group group, String type, String[] field) {
        this.group = group;
        this.type = type;
        this.fields = field;
        this.fieldsString = Arrays.toString(field);
    }
}
