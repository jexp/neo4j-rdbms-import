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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author mh
 * @since 29.03.15
 */
public class Options {

    enum Rename {
           camelcase() {
            public String transform(String source) { return underscoreToCamelCase(source); }
        }, lowerCamelCase() {
            public String transform(String source) { return deCapitalize(underscoreToCamelCase(source)); }
        }, downcase() {
            public String transform(String source) { return camelWhiteSpaceToUnderscore(source).toLowerCase(); }
        }, upcase() {
            public String transform(String source) { return camelWhiteSpaceToUnderscore(source).toUpperCase(); }
        }, none() {
            public String transform(String source) { return source; }
        }, capitalize() {
            public String transform(String source) { return capitalize(source); }
        };

        private static String underscoreToCamelCase(String source) {
            String result = "";
            for (String part : source.split("[ _]+")) {
                result += capitalize(part);
            }
            return result;
        }
        private static String capitalize(String source) {
            return source == null || source.trim().isEmpty() ? source : source.substring(0,1).toUpperCase()+source.substring(1);
        }
        private static String deCapitalize(String source) {
            return source == null || source.trim().isEmpty() ? source : source.substring(0,1).toLowerCase()+source.substring(1);
        }

        private static String camelWhiteSpaceToUnderscore(String source) {
            return source.replaceAll("([a-z]) ?([A-Z])", "$1_$2").replaceAll("[_ ]+", "_");
        }

        public abstract String transform(String source);
    }
    private String database;
    private String schema;
    private String jdbc;
    private Pattern skip;
    private Pattern relationship;
    private Rename labelRename = Rename.camelcase;
    private Rename relationshipRename = Rename.upcase;
    private Rename propertyRename = Rename.lowerCamelCase;

    boolean verify() {
        return false;
    }

    Options setSkipList(String skipList) {
        if (skipList==null || skipList.trim().isEmpty()) return this;
        String normalized = "^("+skipList.replaceAll("\\s*,\\s*", "|").replaceAll("\\.", "\\.").replaceAll("%", ".*")+")";
        this.skip = java.util.regex.Pattern.compile(normalized,Pattern.CASE_INSENSITIVE);
        return this;
    }

    boolean shouldSkip(String table, String field) {
        return skip != null && skip.matcher(table+"."+field).find();
    }

    public String getDatabase() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public String getJdbc() {
        return jdbc;
    }
}
