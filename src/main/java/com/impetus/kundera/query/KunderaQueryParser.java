/*
 * Copyright 2010 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */package com.impetus.kundera.query;

import java.util.StringTokenizer;

/**
 * Parser for handling JPQL Single-String queries. Takes a JPQLQuery and the
 * query string and parses it into its constituent parts, updating the JPQLQuery
 * accordingly with the result that after calling the parse() method the
 * JPQLQuery is populated.
 * 
 * <pre>
 * SELECT [ {result} ]
 * [FROM {candidate-classes} ]
 * [WHERE {filter}]
 * [GROUP BY {grouping-clause} ]
 * [HAVING {having-clause} ]
 * [ORDER BY {ordering-clause}]
 * e.g SELECT c FROM Customer c INNER JOIN c.orders o WHERE c.status = 1
 * </pre>
 * 
 * @author animesh.kumar
 */
public class KunderaQueryParser {

    /** The JPQL query to populate. */
    private KunderaQuery query;

    /** The single-string query string. */
    private String queryString;

    /**
     * Record of the keyword currently being processed, so we can check for out
     * of order keywords.
     */
    private int keywordPosition = -1;

    /**
     * Constructor for the Single-String parser.
     * 
     * @param query
     *            The query
     * @param queryString
     *            The Single-String query
     */
    public KunderaQueryParser(KunderaQuery query, String queryString) {
        this.query = query;
        this.queryString = queryString;
    }

    /**
     * Method to parse the Single-String query.
     */
    public final void parse() {
        new Compiler(new Parser(queryString)).compile();
    }

    /**
     * Method to detect whether this token is a keyword for JPQL Single-String.
     * 
     * @param token
     *            The token
     * 
     * @return Whether it is a keyword
     */
    private boolean isKeyword(String token) {
        // Compare the passed token against the provided keyword list, or their
        // lowercase form
        for (int i = 0; i < KunderaQuery.SINGLE_STRING_KEYWORDS.length; i++) {
            if (token.equalsIgnoreCase(KunderaQuery.SINGLE_STRING_KEYWORDS[i])) {
                return true;
            }
        }
        return false;
    }

    /**
     * Compiler to process keywords contents. In the query the keywords often
     * have content values following them that represent the constituent parts
     * of the query. This takes the keyword and sets the constituent part
     * accordingly.
     */
    private class Compiler {

        /** The tokenizer. */
        private Parser tokenizer;

        // Temporary variable since grouping clause is made up of GROUP BY ...
        // HAVING ...
        /** The grouping clause. */
        private String groupingClause;

        /**
         * Instantiates a new compiler.
         * 
         * @param tokenizer
         *            the tokenizer
         */
        Compiler(Parser tokenizer) {
            this.tokenizer = tokenizer;
        }

        /**
         * Compile.
         */
        private void compile() {
            // TODO Query can start "SELECT", "DELETE" or "UPDATE"
            compileSelect();

            // any keyword after compiling the SELECT is an error
            String keyword = tokenizer.parseKeyword();
            if (keyword != null) {
                if (isKeyword(keyword)) {
                    throw new RuntimeException("out of order keyword: " + keyword);
                } 
            }
        }

        /**
         * Compile select.
         */
        // TODO: reduce Cyclomatic complexity
        private void compileSelect() {
            if (!tokenizer.parseKeywordIgnoreCase("SELECT")) {
                throw new RuntimeException("no select to start");
            }
            compileResult();
            if (tokenizer.parseKeywordIgnoreCase("FROM")) {
                compileFrom();
            }
            if (tokenizer.parseKeywordIgnoreCase("WHERE")) {
                compileWhere();
            }
            if (tokenizer.parseKeywordIgnoreCase("GROUP BY")) {
                compileGroup();
            }
            if (tokenizer.parseKeywordIgnoreCase("HAVING")) {
                compileHaving();
            }
            if (groupingClause != null) {
                query.setGrouping(groupingClause);
            }

            if (tokenizer.parseKeywordIgnoreCase("ORDER BY")) {
                compileOrder();
            }
        }

        /**
         * Compile result.
         */
        private void compileResult() {
            String content = tokenizer.parseContent();
            // content may be empty
            if (content.length() > 0) {
                query.setResult(content);
            }
        }

        /**
         * Compile from.
         */
        private void compileFrom() {
            String content = tokenizer.parseContent();
            // content may be empty
            if (content.length() > 0) {
                query.setFrom(content);
            }
        }

        /**
         * Compile where.
         */
        private void compileWhere() {
            String content = tokenizer.parseContent();
            // content cannot be empty
            if (content.length() == 0) {
                throw new RuntimeException("keyword without value[WHERE]");
            }
            query.setFilter(content);
        }

        /**
         * Compile group.
         */
        private void compileGroup() {
            String content = tokenizer.parseContent();
            // content cannot be empty
            if (content.length() == 0) {
                throw new RuntimeException("keyword without value: GROUP BY");
            }
            groupingClause = content;
        }

        /**
         * Compile having.
         */
        private void compileHaving() {
            String content = tokenizer.parseContent();
            // content cannot be empty
            if (content.length() == 0) {
                throw new RuntimeException("keyword without value: HAVING");
            }
            if (groupingClause != null) {
                groupingClause = groupingClause.trim() + content;
            } else {
                groupingClause = content;
            }
        }

        /**
         * Compile order.
         */
        private void compileOrder() {
            String content = tokenizer.parseContent();
            // content cannot be empty
            if (content.length() == 0) {
                throw new RuntimeException("keyword without value: ORDER BY");
            }
            query.setOrdering(content);
        }
    }

    /**
     * Tokenizer that provides access to current token.
     */
    private class Parser {

        /** tokens. */
        private final String[] tokens;

        /** keywords. */
        private final String[] keywords;

        /** current token cursor position. */
        private int pos = -1;

        /**
         * Constructor.
         * 
         * @param str
         *            the str
         */
        public Parser(String str) {
            StringTokenizer tokenizer = new StringTokenizer(str);
            tokens = new String[tokenizer.countTokens()];
            keywords = new String[tokenizer.countTokens()];
            int i = 0;
            while (tokenizer.hasMoreTokens()) {
                tokens[i++] = tokenizer.nextToken();
            }
            for (i = 0; i < tokens.length; i++) {
                if (isKeyword(tokens[i])) {
                    keywords[i] = tokens[i];
                } else if (i < tokens.length - 1 && isKeyword(tokens[i] + ' ' + tokens[i + 1])) {
                    keywords[i] = tokens[i];
                    i++;
                    keywords[i] = tokens[i];
                }
            }
        }

        /**
         * Parse the content until a keyword is found.
         * 
         * @return the content
         */
        public String parseContent() {
            String content = "";
            while (pos < tokens.length - 1) {
                pos++;
                if (isKeyword(tokens[pos])) {
                    pos--;
                    break;
                } else if (pos < tokens.length - 1 && isKeyword(tokens[pos] + ' ' + tokens[pos + 1])) {
                    pos--;
                    break;
                } else {
                    if (content.length() == 0) {
                        content = tokens[pos];
                    } else {
                        content += " " + tokens[pos];
                    }
                }
            }
            return content;
        }

        /**
         * Parse the next token looking for a keyword. The cursor position is
         * skipped in one tick if a keyword is found
         * 
         * @param keyword
         *            the searched keyword
         * 
         * @return true if the keyword
         */
        public boolean parseKeyword(String keyword) {
            if (pos < tokens.length - 1) {
                pos++;
                if (keywords[pos] != null) {
                    if (keywords[pos].equals(keyword)) {
                        return true;
                    }
                    if (keyword.indexOf(' ') > -1) {
                        if (pos < keywords.length - 1) {
                            if ((keywords[pos] + ' ' + keywords[pos + 1]).equals(keyword)) {
                                pos++;
                                return true;
                            }
                        }
                    }
                }
                pos--;
            }
            return false;
        }

        /**
         * Parse the next token looking for a keyword. The cursor position is
         * skipped in one tick if a keyword is found
         * 
         * @param keyword
         *            the searched keyword
         * 
         * @return true if the keyword
         */
        public boolean parseKeywordIgnoreCase(String keyword) {
            if (pos < tokens.length - 1) {
                pos++;
                if (keywords[pos] != null) {
                    if (keywords[pos].equalsIgnoreCase(keyword)) {
                        return true;
                    }
                    if (keyword.indexOf(' ') > -1) {
                        if ((keywords[pos] + ' ' + keywords[pos + 1]).equalsIgnoreCase(keyword)) {
                            pos++;
                            return true;
                        }
                    }
                }
                pos--;
            }
            return false;
        }

        /**
         * Parse the next token looking for a keyword. The cursor position is
         * skipped in one tick if a keyword is found
         * 
         * @return the parsed keyword or null
         */
        public String parseKeyword() {
            if (pos < tokens.length - 1) {
                pos++;
                if (keywords[pos] != null) {
                    return keywords[pos];
                }
                pos--;
            }
            return null;
        }
    }
}
