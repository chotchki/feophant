use crate::engine::objects::Attribute;

use super::base_sql_types::BaseSqlTypesMapper;
use std::{
    fmt::{self, Display, Formatter},
    ops::Deref,
};

/// Wrapper type that implements custom types including for tables.
#[derive(Clone, Debug, PartialEq)]
// TODO I'm not super happy with the use of Vec but I need the order preseved and easy acess to the offset.
pub struct SqlTypeDefinition(pub Vec<(String, BaseSqlTypesMapper)>);

impl SqlTypeDefinition {
    pub fn new(attributes: &[Attribute]) -> SqlTypeDefinition {
        SqlTypeDefinition(
            attributes
                .iter()
                .map(|a| (a.name.clone(), a.sql_type.clone()))
                .collect(),
        )
    }
}

impl Deref for SqlTypeDefinition {
    type Target = Vec<(String, BaseSqlTypesMapper)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for SqlTypeDefinition {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:#?}", self)
    }
}
