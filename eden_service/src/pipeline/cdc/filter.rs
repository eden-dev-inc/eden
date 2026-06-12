//! SQL WHERE filter engine for CDC row evaluation.
//!
//! Parses a SQL WHERE clause into an AST at snapshot creation time,
//! then evaluates it against each decoded WAL row during CDC streaming.

use eden_core::error::EpError;
use serde_json::Value;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value as SqlValue};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;

/// A compiled SQL WHERE filter that can evaluate row data.
#[derive(Debug, Clone)]
pub struct WhereFilter {
    ast: Expr,
    /// The original SQL WHERE clause text (for injection into SELECT queries).
    source_sql: String,
}

impl WhereFilter {
    /// Parse a SQL WHERE clause string into a reusable filter.
    ///
    /// Validates syntax and rejects unsupported constructs (subqueries, aggregates, window functions).
    pub fn parse(filter_sql: &str) -> Result<Self, EpError> {
        let wrapped = format!("SELECT 1 WHERE {filter_sql}");
        let dialect = PostgreSqlDialect {};

        let statements = Parser::parse_sql(&dialect, &wrapped).map_err(|e| EpError::parse(format!("Invalid filter syntax: {e}")))?;

        if statements.is_empty() {
            return Err(EpError::parse("Filter produced no valid SQL statement"));
        }

        // Extract the WHERE clause expression from the parsed SELECT
        let where_expr = match &statements[0] {
            sqlparser::ast::Statement::Query(query) => match query.body.as_ref() {
                sqlparser::ast::SetExpr::Select(select) => {
                    select.selection.clone().ok_or_else(|| EpError::parse("No WHERE clause found in filter"))
                }
                _ => Err(EpError::parse("Unexpected query structure")),
            },
            _ => Err(EpError::parse("Filter did not parse as a query")),
        }?;

        // Validate: no subqueries
        reject_subqueries(&where_expr)?;

        Ok(Self { ast: where_expr, source_sql: filter_sql.to_string() })
    }

    /// Evaluate the filter against a row represented as column name → JSON value.
    ///
    /// Returns `true` if the row matches the filter, `false` otherwise.
    pub fn evaluate(&self, row: &HashMap<String, Value>) -> Result<bool, EpError> {
        let result = eval_expr(&self.ast, row)?;
        to_bool(&result)
    }

    /// Get the original SQL WHERE clause text.
    pub fn sql(&self) -> &str {
        &self.source_sql
    }

    /// Get a reference to the parsed AST (for debugging/testing).
    pub fn ast(&self) -> &Expr {
        &self.ast
    }
}

/// Internal value representation for expression evaluation.
#[derive(Debug, Clone, PartialEq)]
enum EvalValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

impl EvalValue {
    fn from_json(v: &Value) -> Self {
        match v {
            Value::Null => Self::Null,
            Value::Bool(b) => Self::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Self::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Self::Float(f)
                } else {
                    Self::Text(n.to_string())
                }
            }
            Value::String(s) => Self::Text(s.clone()),
            _ => Self::Text(v.to_string()),
        }
    }

    fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Coerce to f64 for numeric comparisons.
    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int(i) => Some(*i as f64),
            Self::Float(f) => Some(*f),
            Self::Text(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    /// Coerce to string for text operations.
    fn as_text(&self) -> Option<String> {
        match self {
            Self::Text(s) => Some(s.clone()),
            Self::Int(i) => Some(i.to_string()),
            Self::Float(f) => Some(f.to_string()),
            Self::Bool(b) => Some(b.to_string()),
            Self::Null => None,
        }
    }
}

fn to_bool(v: &EvalValue) -> Result<bool, EpError> {
    match v {
        EvalValue::Bool(b) => Ok(*b),
        EvalValue::Null => Ok(false),
        EvalValue::Int(i) => Ok(*i != 0),
        _ => Err(EpError::parse(format!("Expected boolean result from filter, got: {v:?}"))),
    }
}

/// Evaluate a SQL expression against a row.
fn eval_expr(expr: &Expr, row: &HashMap<String, Value>) -> Result<EvalValue, EpError> {
    match expr {
        // Column reference: look up in row
        Expr::Identifier(ident) => {
            let col_name = ident.value.to_lowercase();
            match row.get(&col_name) {
                Some(v) => Ok(EvalValue::from_json(v)),
                None => {
                    // Also try original case
                    match row.get(&ident.value) {
                        Some(v) => Ok(EvalValue::from_json(v)),
                        None => Ok(EvalValue::Null),
                    }
                }
            }
        }

        // Compound identifier (e.g., table.column)
        Expr::CompoundIdentifier(parts) => {
            let col_name = parts.last().map(|i| i.value.to_lowercase()).unwrap_or_default();
            match row.get(&col_name) {
                Some(v) => Ok(EvalValue::from_json(v)),
                None => Ok(EvalValue::Null),
            }
        }

        // Literal values
        Expr::Value(v) => eval_sql_value(v),

        // Binary operations (=, !=, <, >, AND, OR, +, -, etc.)
        Expr::BinaryOp { left, op, right } => eval_binary_op(left, op, right, row),

        // Unary operations (NOT, -)
        Expr::UnaryOp { op, expr } => eval_unary_op(op, expr, row),

        // IS NULL / IS NOT NULL
        Expr::IsNull(inner) => {
            let v = eval_expr(inner, row)?;
            Ok(EvalValue::Bool(v.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let v = eval_expr(inner, row)?;
            Ok(EvalValue::Bool(!v.is_null()))
        }

        // IN list: expr IN (val1, val2, ...)
        Expr::InList { expr, list, negated } => {
            let val = eval_expr(expr, row)?;
            if val.is_null() {
                return Ok(EvalValue::Bool(false));
            }
            let mut found = false;
            for item in list {
                let item_val = eval_expr(item, row)?;
                if compare_eq(&val, &item_val) {
                    found = true;
                    break;
                }
            }
            Ok(EvalValue::Bool(if *negated { !found } else { found }))
        }

        // BETWEEN: expr BETWEEN low AND high
        Expr::Between { expr, negated, low, high } => {
            let val = eval_expr(expr, row)?;
            let low_val = eval_expr(low, row)?;
            let high_val = eval_expr(high, row)?;

            if val.is_null() || low_val.is_null() || high_val.is_null() {
                return Ok(EvalValue::Bool(false));
            }

            let in_range = compare_ord(&val, &low_val) >= 0 && compare_ord(&val, &high_val) <= 0;
            Ok(EvalValue::Bool(if *negated { !in_range } else { in_range }))
        }

        // LIKE / ILIKE
        Expr::Like { negated, expr, pattern, escape_char, any: _ } => eval_like(expr, pattern, *negated, false, escape_char, row),

        Expr::ILike { negated, expr, pattern, escape_char, any: _ } => eval_like(expr, pattern, *negated, true, escape_char, row),

        // IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE
        Expr::IsTrue(inner) => {
            let v = eval_expr(inner, row)?;
            Ok(EvalValue::Bool(matches!(v, EvalValue::Bool(true))))
        }
        Expr::IsFalse(inner) => {
            let v = eval_expr(inner, row)?;
            Ok(EvalValue::Bool(matches!(v, EvalValue::Bool(false))))
        }
        Expr::IsNotTrue(inner) => {
            let v = eval_expr(inner, row)?;
            Ok(EvalValue::Bool(!matches!(v, EvalValue::Bool(true))))
        }
        Expr::IsNotFalse(inner) => {
            let v = eval_expr(inner, row)?;
            Ok(EvalValue::Bool(!matches!(v, EvalValue::Bool(false))))
        }

        // Nested expression (parenthesized)
        Expr::Nested(inner) => eval_expr(inner, row),

        // CAST(expr AS type) — evaluate the inner expression, ignore the cast for now
        Expr::Cast { expr, .. } => eval_expr(expr, row),

        other => Err(EpError::parse(format!("Unsupported expression in filter: {other}"))),
    }
}

fn eval_sql_value(v: &sqlparser::ast::ValueWithSpan) -> Result<EvalValue, EpError> {
    match &v.value {
        SqlValue::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(EvalValue::Int(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(EvalValue::Float(f))
            } else {
                Ok(EvalValue::Text(n.clone()))
            }
        }
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => Ok(EvalValue::Text(s.clone())),
        SqlValue::Boolean(b) => Ok(EvalValue::Bool(*b)),
        SqlValue::Null => Ok(EvalValue::Null),
        other => Ok(EvalValue::Text(format!("{other}"))),
    }
}

fn eval_binary_op(left: &Expr, op: &BinaryOperator, right: &Expr, row: &HashMap<String, Value>) -> Result<EvalValue, EpError> {
    let lhs = eval_expr(left, row)?;
    let rhs = eval_expr(right, row)?;

    match op {
        // Logical
        BinaryOperator::And => {
            let l = to_bool(&lhs)?;
            let r = to_bool(&rhs)?;
            Ok(EvalValue::Bool(l && r))
        }
        BinaryOperator::Or => {
            let l = to_bool(&lhs)?;
            let r = to_bool(&rhs)?;
            Ok(EvalValue::Bool(l || r))
        }

        // Equality
        BinaryOperator::Eq => {
            if lhs.is_null() || rhs.is_null() {
                Ok(EvalValue::Bool(false))
            } else {
                Ok(EvalValue::Bool(compare_eq(&lhs, &rhs)))
            }
        }
        BinaryOperator::NotEq => {
            if lhs.is_null() || rhs.is_null() {
                Ok(EvalValue::Bool(false))
            } else {
                Ok(EvalValue::Bool(!compare_eq(&lhs, &rhs)))
            }
        }

        // Ordering — NULL comparisons always return false per SQL semantics
        BinaryOperator::Lt => {
            if lhs.is_null() || rhs.is_null() {
                return Ok(EvalValue::Bool(false));
            }
            Ok(EvalValue::Bool(compare_ord(&lhs, &rhs) < 0))
        }
        BinaryOperator::LtEq => {
            if lhs.is_null() || rhs.is_null() {
                return Ok(EvalValue::Bool(false));
            }
            Ok(EvalValue::Bool(compare_ord(&lhs, &rhs) <= 0))
        }
        BinaryOperator::Gt => {
            if lhs.is_null() || rhs.is_null() {
                return Ok(EvalValue::Bool(false));
            }
            Ok(EvalValue::Bool(compare_ord(&lhs, &rhs) > 0))
        }
        BinaryOperator::GtEq => {
            if lhs.is_null() || rhs.is_null() {
                return Ok(EvalValue::Bool(false));
            }
            Ok(EvalValue::Bool(compare_ord(&lhs, &rhs) >= 0))
        }

        // Arithmetic
        BinaryOperator::Plus => eval_arithmetic(&lhs, &rhs, |a, b| a + b),
        BinaryOperator::Minus => eval_arithmetic(&lhs, &rhs, |a, b| a - b),
        BinaryOperator::Multiply => eval_arithmetic(&lhs, &rhs, |a, b| a * b),
        BinaryOperator::Divide => {
            if let Some(r) = rhs.as_f64() {
                if r == 0.0 {
                    return Err(EpError::parse("Division by zero in filter expression"));
                }
            }
            eval_arithmetic(&lhs, &rhs, |a, b| a / b)
        }

        // String concatenation
        BinaryOperator::StringConcat => {
            let l = lhs.as_text().unwrap_or_default();
            let r = rhs.as_text().unwrap_or_default();
            Ok(EvalValue::Text(format!("{l}{r}")))
        }

        other => Err(EpError::parse(format!("Unsupported binary operator in filter: {other}"))),
    }
}

fn eval_unary_op(op: &UnaryOperator, expr: &Expr, row: &HashMap<String, Value>) -> Result<EvalValue, EpError> {
    let val = eval_expr(expr, row)?;
    match op {
        UnaryOperator::Not => {
            let b = to_bool(&val)?;
            Ok(EvalValue::Bool(!b))
        }
        UnaryOperator::Minus => match val {
            EvalValue::Int(i) => Ok(EvalValue::Int(-i)),
            EvalValue::Float(f) => Ok(EvalValue::Float(-f)),
            _ => Err(EpError::parse("Cannot negate non-numeric value")),
        },
        UnaryOperator::Plus => Ok(val),
        other => Err(EpError::parse(format!("Unsupported unary operator in filter: {other}"))),
    }
}

fn eval_like(
    expr: &Expr,
    pattern: &Expr,
    negated: bool,
    case_insensitive: bool,
    escape_char: &Option<String>,
    row: &HashMap<String, Value>,
) -> Result<EvalValue, EpError> {
    let val = eval_expr(expr, row)?;
    let pat = eval_expr(pattern, row)?;

    let val_str = match val.as_text() {
        Some(s) => s,
        None => return Ok(EvalValue::Bool(false)),
    };
    let pat_str = match pat.as_text() {
        Some(s) => s,
        None => return Ok(EvalValue::Bool(false)),
    };

    let escape = escape_char.as_ref().and_then(|s| s.chars().next()).unwrap_or('\\');
    let matches = sql_like_match(&val_str, &pat_str, case_insensitive, escape);

    Ok(EvalValue::Bool(if negated { !matches } else { matches }))
}

/// Match a value against a SQL LIKE pattern.
fn sql_like_match(value: &str, pattern: &str, case_insensitive: bool, escape: char) -> bool {
    let (val, pat) = if case_insensitive {
        (value.to_lowercase(), pattern.to_lowercase())
    } else {
        (value.to_string(), pattern.to_string())
    };

    let val_chars: Vec<char> = val.chars().collect();
    let pat_chars: Vec<char> = pat.chars().collect();

    // Use memoization table to avoid exponential worst-case (e.g., '%_%_%_...' patterns)
    let mut memo = vec![vec![None::<bool>; pat_chars.len() + 1]; val_chars.len() + 1];
    like_dp(&val_chars, &pat_chars, 0, 0, escape, &mut memo)
}

/// Memoized LIKE matcher. `memo[vi][pi]` caches the result for position (vi, pi).
fn like_dp(val: &[char], pat: &[char], vi: usize, pi: usize, escape: char, memo: &mut [Vec<Option<bool>>]) -> bool {
    if pi == pat.len() {
        return vi == val.len();
    }

    if let Some(cached) = memo[vi][pi] {
        return cached;
    }

    let result = if pat[pi] == escape && pi + 1 < pat.len() {
        // Handle escape character
        vi < val.len() && val[vi] == pat[pi + 1] && like_dp(val, pat, vi + 1, pi + 2, escape, memo)
    } else {
        match pat[pi] {
            '%' => {
                // Match zero or more characters
                let mut matched = false;
                for i in vi..=val.len() {
                    if like_dp(val, pat, i, pi + 1, escape, memo) {
                        matched = true;
                        break;
                    }
                }
                matched
            }
            '_' => {
                // Match exactly one character
                vi < val.len() && like_dp(val, pat, vi + 1, pi + 1, escape, memo)
            }
            c => vi < val.len() && val[vi] == c && like_dp(val, pat, vi + 1, pi + 1, escape, memo),
        }
    };

    memo[vi][pi] = Some(result);
    result
}

/// Compare two values for equality with type coercion.
fn compare_eq(a: &EvalValue, b: &EvalValue) -> bool {
    match (a, b) {
        (EvalValue::Null, _) | (_, EvalValue::Null) => false,
        (EvalValue::Bool(a), EvalValue::Bool(b)) => a == b,
        (EvalValue::Int(a), EvalValue::Int(b)) => a == b,
        (EvalValue::Float(a), EvalValue::Float(b)) => (a - b).abs() < f64::EPSILON,
        (EvalValue::Text(a), EvalValue::Text(b)) => a == b,
        // Cross-type numeric comparison
        (EvalValue::Int(a), EvalValue::Float(b)) | (EvalValue::Float(b), EvalValue::Int(a)) => (*a as f64 - b).abs() < f64::EPSILON,
        // Text to numeric coercion for comparisons like column = '123'
        _ => {
            if let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) {
                (a - b).abs() < f64::EPSILON
            } else {
                a.as_text() == b.as_text()
            }
        }
    }
}

/// Compare two values for ordering. Returns -1, 0, or 1.
fn compare_ord(a: &EvalValue, b: &EvalValue) -> i8 {
    if a.is_null() || b.is_null() {
        return 0; // NULL comparisons return false in SQL, but we need a number
    }

    // Try numeric comparison first
    if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
        return if af < bf {
            -1
        } else if af > bf {
            1
        } else {
            0
        };
    }

    // Fall back to text comparison
    match (a.as_text(), b.as_text()) {
        (Some(at), Some(bt)) => at.cmp(&bt) as i8,
        _ => 0,
    }
}

fn eval_arithmetic(lhs: &EvalValue, rhs: &EvalValue, op: fn(f64, f64) -> f64) -> Result<EvalValue, EpError> {
    match (lhs.as_f64(), rhs.as_f64()) {
        (Some(a), Some(b)) => {
            let result = op(a, b);
            // Return Int if both inputs were Int and result is whole
            if matches!(lhs, EvalValue::Int(_)) && matches!(rhs, EvalValue::Int(_)) && result.fract() == 0.0 {
                Ok(EvalValue::Int(result as i64))
            } else {
                Ok(EvalValue::Float(result))
            }
        }
        _ => Err(EpError::parse("Cannot perform arithmetic on non-numeric values")),
    }
}

/// Reject subqueries, aggregates, and window functions in filter expressions.
fn reject_subqueries(expr: &Expr) -> Result<(), EpError> {
    match expr {
        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
            Err(EpError::parse("Subqueries are not allowed in filter expressions"))
        }
        Expr::Function(f) => {
            let name = f.name.to_string().to_lowercase();
            let aggregates = ["count", "sum", "avg", "min", "max", "array_agg", "string_agg"];
            if aggregates.contains(&name.as_str()) {
                return Err(EpError::parse(format!("Aggregate function '{name}' is not allowed in filter expressions")));
            }
            if f.over.is_some() {
                return Err(EpError::parse("Window functions are not allowed in filter expressions"));
            }
            Ok(())
        }
        Expr::BinaryOp { left, right, .. } => {
            reject_subqueries(left)?;
            reject_subqueries(right)
        }
        Expr::UnaryOp { expr, .. } => reject_subqueries(expr),
        Expr::Nested(inner) => reject_subqueries(inner),
        Expr::InList { expr, list, .. } => {
            reject_subqueries(expr)?;
            for item in list {
                reject_subqueries(item)?;
            }
            Ok(())
        }
        Expr::Between { expr, low, high, .. } => {
            reject_subqueries(expr)?;
            reject_subqueries(low)?;
            reject_subqueries(high)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            reject_subqueries(expr)?;
            reject_subqueries(pattern)
        }
        Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner) => reject_subqueries(inner),
        Expr::Cast { expr, .. } => reject_subqueries(expr),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_row(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
        pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

    #[test]
    fn test_simple_equality() {
        let filter = WhereFilter::parse("status = 'completed'").expect("parse");
        let row = make_row(vec![("status", json!("completed"))]);
        assert!(filter.evaluate(&row).expect("eval"));

        let row2 = make_row(vec![("status", json!("pending"))]);
        assert!(!filter.evaluate(&row2).expect("eval"));
    }

    #[test]
    fn test_numeric_comparison() {
        let filter = WhereFilter::parse("total_amount > 100.00").expect("parse");
        let row = make_row(vec![("total_amount", json!(150.0))]);
        assert!(filter.evaluate(&row).expect("eval"));

        let row2 = make_row(vec![("total_amount", json!(50.0))]);
        assert!(!filter.evaluate(&row2).expect("eval"));
    }

    #[test]
    fn test_and_or() {
        let filter = WhereFilter::parse("status = 'completed' AND total > 100").expect("parse");
        let row = make_row(vec![("status", json!("completed")), ("total", json!(200))]);
        assert!(filter.evaluate(&row).expect("eval"));

        let row2 = make_row(vec![("status", json!("completed")), ("total", json!(50))]);
        assert!(!filter.evaluate(&row2).expect("eval"));
    }

    #[test]
    fn test_in_list() {
        let filter = WhereFilter::parse("status IN ('completed', 'shipped')").expect("parse");
        let row = make_row(vec![("status", json!("shipped"))]);
        assert!(filter.evaluate(&row).expect("eval"));

        let row2 = make_row(vec![("status", json!("pending"))]);
        assert!(!filter.evaluate(&row2).expect("eval"));
    }

    #[test]
    fn test_between() {
        let filter = WhereFilter::parse("price BETWEEN 10 AND 50").expect("parse");
        let row = make_row(vec![("price", json!(25))]);
        assert!(filter.evaluate(&row).expect("eval"));

        let row2 = make_row(vec![("price", json!(100))]);
        assert!(!filter.evaluate(&row2).expect("eval"));
    }

    #[test]
    fn test_is_null() {
        let filter = WhereFilter::parse("email IS NOT NULL").expect("parse");
        let row = make_row(vec![("email", json!("user@example.com"))]);
        assert!(filter.evaluate(&row).expect("eval"));

        let row2 = make_row(vec![("email", Value::Null)]);
        assert!(!filter.evaluate(&row2).expect("eval"));
    }

    #[test]
    fn test_like() {
        let filter = WhereFilter::parse("name LIKE 'John%'").expect("parse");
        let row = make_row(vec![("name", json!("John Doe"))]);
        assert!(filter.evaluate(&row).expect("eval"));

        let row2 = make_row(vec![("name", json!("Jane Doe"))]);
        assert!(!filter.evaluate(&row2).expect("eval"));
    }

    #[test]
    fn test_ilike() {
        let filter = WhereFilter::parse("name ILIKE '%john%'").expect("parse");
        let row = make_row(vec![("name", json!("JOHN DOE"))]);
        assert!(filter.evaluate(&row).expect("eval"));
    }

    #[test]
    fn test_not() {
        let filter = WhereFilter::parse("NOT status = 'cancelled'").expect("parse");
        let row = make_row(vec![("status", json!("completed"))]);
        assert!(filter.evaluate(&row).expect("eval"));

        let row2 = make_row(vec![("status", json!("cancelled"))]);
        assert!(!filter.evaluate(&row2).expect("eval"));
    }

    #[test]
    fn test_complex_filter() {
        let filter =
            WhereFilter::parse("status IN ('completed', 'shipped') AND total_amount > 100.00 AND customer_name LIKE 'A%'").expect("parse");

        let row = make_row(vec![
            ("status", json!("completed")),
            ("total_amount", json!(250.0)),
            ("customer_name", json!("Alice")),
        ]);
        assert!(filter.evaluate(&row).expect("eval"));

        let row2 = make_row(vec![
            ("status", json!("completed")),
            ("total_amount", json!(250.0)),
            ("customer_name", json!("Bob")),
        ]);
        assert!(!filter.evaluate(&row2).expect("eval"));
    }

    #[test]
    fn test_reject_subquery() {
        let result = WhereFilter::parse("id IN (SELECT id FROM other_table)");
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_column_is_null() {
        let filter = WhereFilter::parse("missing_col = 'test'").expect("parse");
        let row = make_row(vec![("other_col", json!("value"))]);
        // Missing column evaluates to NULL, NULL = 'test' → false
        assert!(!filter.evaluate(&row).expect("eval"));
    }

    #[test]
    fn test_arithmetic() {
        let filter = WhereFilter::parse("price * quantity > 1000").expect("parse");
        let row = make_row(vec![("price", json!(50)), ("quantity", json!(25))]);
        assert!(filter.evaluate(&row).expect("eval"));

        let row2 = make_row(vec![("price", json!(10)), ("quantity", json!(5))]);
        assert!(!filter.evaluate(&row2).expect("eval"));
    }

    #[test]
    fn test_not_between() {
        let filter = WhereFilter::parse("age NOT BETWEEN 18 AND 65").expect("parse");
        let row = make_row(vec![("age", json!(70))]);
        assert!(filter.evaluate(&row).expect("eval"));

        let row2 = make_row(vec![("age", json!(30))]);
        assert!(!filter.evaluate(&row2).expect("eval"));
    }

    #[test]
    fn test_null_ordering_returns_false() {
        // In SQL, NULL >= 5, NULL < 5, etc. all return false (NULL)
        let filter = WhereFilter::parse("val >= 5").expect("parse");
        let row = make_row(vec![("val", Value::Null)]);
        assert!(!filter.evaluate(&row).expect("eval"));

        let filter2 = WhereFilter::parse("val < 10").expect("parse");
        assert!(!filter2.evaluate(&row).expect("eval"));

        // Also test with missing column (evaluates to NULL)
        let filter3 = WhereFilter::parse("missing >= 0").expect("parse");
        let row2 = make_row(vec![("other", json!(1))]);
        assert!(!filter3.evaluate(&row2).expect("eval"));
    }

    #[test]
    fn test_invalid_syntax() {
        let result = WhereFilter::parse("status === 'bad'");
        assert!(result.is_err());
    }
}
