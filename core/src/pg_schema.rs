#[derive(Debug, Clone)]
pub enum TypeSize {
    Fixed(usize),
    Variable,
}

#[derive(Debug, Clone)]
pub enum PostgresType {
    Bool,
    Bytea,
    Int8,
    Int2,
    Int4,
    Text,
    Float4,
    Float8,
    Date,
    Time,
    Timestamp,
    Interval,
    List(Box<Column>),
}

impl PostgresType {
    pub const fn size(&self) -> TypeSize {
        match &self {
            PostgresType::Bool => TypeSize::Fixed(1),
            PostgresType::Bytea => TypeSize::Variable,
            PostgresType::Int2 => TypeSize::Fixed(2),
            PostgresType::Int4 => TypeSize::Fixed(4),
            PostgresType::Int8 => TypeSize::Fixed(8),
            PostgresType::Text => TypeSize::Variable,
            PostgresType::Float4 => TypeSize::Fixed(4),
            PostgresType::Float8 => TypeSize::Fixed(8),
            PostgresType::Date => TypeSize::Fixed(4),
            PostgresType::Time => TypeSize::Fixed(8),
            PostgresType::Timestamp => TypeSize::Fixed(8),
            PostgresType::Interval => TypeSize::Fixed(16),
            PostgresType::List(_) => TypeSize::Variable,
        }
    }
    pub fn name(&self) -> Option<String> {
        let v = match &self {
            PostgresType::Bool => "BOOL".to_string(),
            PostgresType::Bytea => "BYTEA".to_string(),
            PostgresType::Int8 => "INT8".to_string(),
            PostgresType::Int2 => "INT2".to_string(),
            PostgresType::Int4 => "INT4".to_string(),
            PostgresType::Text => "TEXT".to_string(),
            PostgresType::Float4 => "FLOAT4".to_string(),
            PostgresType::Float8 => "FLOAT8".to_string(),
            PostgresType::Date => "DATE".to_string(),
            PostgresType::Time => "TIME".to_string(),
            PostgresType::Timestamp => "TIMESTAMP".to_string(),
            PostgresType::Interval => "INTERVAL".to_string(),
            PostgresType::List(inner) => {
                // arrays of structs and such are not supported
                let inner_tp = inner.data_type.name().unwrap();
                format!("{inner_tp}[]")
            }
        };
        Some(v)
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: PostgresType,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct PostgresSchema {
    pub columns: Vec<Column>,
}
