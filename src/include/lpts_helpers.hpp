#pragma once

#include "duckdb.hpp"

#include <sstream>
#include <string>
#include <vector>

namespace duckdb {

/// Convert a vector of strings into a separated list (e.g. "a, b, c").
string VecToSeparatedList(vector<string> input_list, const string &separator = ", ");

/// Escape single quotes in a string by doubling them (e.g. "it's" -> "it''s").
string EscapeSingleQuotes(const string &input);

/// Convert a SQL string to lowercase, preserving case inside string literals.
string SQLToLowercase(const string &sql);

/// Remove redundant whitespace from a query string.
void RemoveRedundantWhitespaces(string &query);

} // namespace duckdb
