use super::base_sql_types::BaseSqlTypesMapper;

///This defines types and will be used to interpret a SqlTuple.
///Arrays have been lowered into the BaseSqlTypes
#[derive(Clone, Debug)]
pub enum SqlTypeDefinition<'a> {
    Base(BaseSqlTypesMapper<'a>),
    Composite(Vec<(&'a str, SqlTypeDefinition<'a>)>),
}
