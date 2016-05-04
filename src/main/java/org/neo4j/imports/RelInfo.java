/**
 * Copyright (c) 2015 Michael Hunger
 *
 * This file is part of Relational to Neo4j Importer.
 *
 *  Relational to Neo4j Importer is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Relational to Neo4j Importer is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Relational to Neo4j Importer.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.imports;

import org.neo4j.unsafe.impl.batchimport.input.Group;

import java.util.List;

/**
 * @author mh
 * @since 01.03.15
 */
public class RelInfo {
    public final Group group;
    public final String type;
    public final List<String> fields;
    public final String fieldsString;

    public RelInfo(Group group, String type, List<String> field) {
        this.group = group;
        this.type = type;
        this.fields = field;
        this.fieldsString = field.toString();
    }
}
