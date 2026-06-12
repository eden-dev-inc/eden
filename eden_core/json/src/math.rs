#[derive(Debug)]
pub struct MathInput {
    values: Vec<f64>,
    kind: MathOperation,
}

#[derive(Debug)]
pub enum MathOperation {
    Add,
    Multiply,
    Average,
    Min,
    Max,
}

pub fn math_operation(input: MathInput) -> Option<f64> {
    if input.values.is_empty() {
        return None;
    }

    match input.kind {
        MathOperation::Add => Some(input.values.iter().sum()),
        MathOperation::Multiply => Some(input.values.iter().product()),
        MathOperation::Average => Some(input.values.iter().sum::<f64>() / input.values.len() as f64),
        MathOperation::Min => input.values.iter().copied().reduce(f64::min),
        MathOperation::Max => input.values.iter().copied().reduce(f64::max),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        let input = MathInput { values: vec![1.0, -2.0, 3.0], kind: MathOperation::Add };
        let result = math_operation(input).unwrap_or_default();
        assert_eq!(result, 2.0);
    }

    #[test]
    fn test_multiply() {
        let input = MathInput { values: vec![2.0, 3.0, 4.0], kind: MathOperation::Multiply };
        let result = math_operation(input).unwrap_or_default();
        assert_eq!(result, 24.0);
    }

    #[test]
    fn test_average() {
        let input = MathInput { values: vec![2.0, 4.0, 6.0], kind: MathOperation::Average };
        let result = math_operation(input).unwrap_or_default();
        assert_eq!(result, 4.0);
    }

    #[test]
    fn test_min() {
        let input = MathInput { values: vec![3.0, 1.0, 4.0], kind: MathOperation::Min };
        let result = math_operation(input).unwrap_or_default();
        assert_eq!(result, 1.0);
    }

    #[test]
    fn test_max() {
        let input = MathInput { values: vec![3.0, 1.0, 4.0], kind: MathOperation::Max };
        let result = math_operation(input).unwrap_or_default();
        assert_eq!(result, 4.0);
    }

    #[test]
    fn test_empty_input() {
        let input = MathInput { values: vec![], kind: MathOperation::Add };
        assert!(math_operation(input).is_none());
    }
}
