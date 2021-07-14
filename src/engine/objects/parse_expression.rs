#[derive(Clone, Debug, PartialEq)]
pub enum ParseExpression {
    String(String),
    Null(),
}
