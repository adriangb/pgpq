#[derive(Debug, Clone, PartialEq)]
pub enum TypeSize {
    Fixed(usize),
    Variable,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PostgresType {
    Bool,
    Bytea,
    Int8,
    Int2,
    Int4,
    Char,
    Text,
    Json,
    Jsonb,
    Float4,
    Float8,
    Date,
    Time,
    Timestamp,
    Interval,
    List(Box<Column>),
    UserDefined { fields: Vec<Box<Column>> }, // User-defined type, e.g. a struct
}

impl PostgresType {
    pub const fn size(&self) -> TypeSize {
        match &self {
            PostgresType::Bool => TypeSize::Fixed(1),
            PostgresType::Bytea => TypeSize::Variable,
            PostgresType::Int2 => TypeSize::Fixed(2),
            PostgresType::Int4 => TypeSize::Fixed(4),
            PostgresType::Int8 => TypeSize::Fixed(8),
            PostgresType::Char => TypeSize::Fixed(2),
            PostgresType::Text => TypeSize::Variable,
            PostgresType::Json => TypeSize::Variable,
            PostgresType::Jsonb => TypeSize::Variable,
            PostgresType::Float4 => TypeSize::Fixed(4),
            PostgresType::Float8 => TypeSize::Fixed(8),
            PostgresType::Date => TypeSize::Fixed(4),
            PostgresType::Time => TypeSize::Fixed(8),
            PostgresType::Timestamp => TypeSize::Fixed(8),
            PostgresType::Interval => TypeSize::Fixed(16),
            PostgresType::List(_) => TypeSize::Variable,
            PostgresType::UserDefined { .. } => TypeSize::Variable,
        }
    }
    pub fn oid(&self) -> Option<u32> {
        match &self {
            PostgresType::Bool => Some(16),
            PostgresType::Bytea => Some(17),
            PostgresType::Int8 => Some(20),
            PostgresType::Int2 => Some(21),
            PostgresType::Int4 => Some(23),
            PostgresType::Char => Some(18),
            PostgresType::Text => Some(25),
            PostgresType::Json => Some(3802),
            PostgresType::Jsonb => Some(3802),
            PostgresType::Float4 => Some(700),
            PostgresType::Float8 => Some(701),
            PostgresType::Date => Some(1082),
            PostgresType::Time => Some(1083),
            PostgresType::Timestamp => Some(1114),
            PostgresType::Interval => Some(1186),
            PostgresType::List(_) => None,
            PostgresType::UserDefined { .. } => Some(16385), // arbitrary dummy oid
        }
    }
    pub fn name(&self) -> Option<String> {
        let v = match &self {
            PostgresType::Bool => "BOOL".to_string(),
            PostgresType::Bytea => "BYTEA".to_string(),
            PostgresType::Int8 => "INT8".to_string(),
            PostgresType::Int2 => "INT2".to_string(),
            PostgresType::Int4 => "INT4".to_string(),
            PostgresType::Char => "CHAR".to_string(),
            PostgresType::Text => "TEXT".to_string(),
            PostgresType::Json => "JSON".to_string(),
            PostgresType::Jsonb => "JSONB".to_string(),
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
            PostgresType::UserDefined { .. } => "userdefined_t".to_string(),
        };
        Some(v)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: PostgresType,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct PostgresSchema {
    pub columns: Vec<Column>,
}

impl PostgresSchema {
    pub fn ddl(&self, table_name: &str) -> String {
        // Collect user-defined type DDLs and a mapping from column path to type name
        fn collect_types(
            col: &Column,
            types: &mut Vec<(String, Vec<(String, String)>)>,
            type_map: &mut std::collections::HashMap<*const Column, String>,
        ) {
            if let PostgresType::UserDefined { fields } = &col.data_type {
                for field in fields.iter() {
                    collect_types(field, types, type_map); // Recursively collect nested types first
                }
                let type_name = format!("{}_t", col.name);
                type_map.insert(col as *const _, type_name.clone());
                let field_ddls = fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| {
                        let field_name = format!("f{}", i);
                        let field_type = match &field.data_type {
                            PostgresType::UserDefined { .. } => type_map
                                .get(&(field.as_ref() as *const _))
                                .cloned()
                                .unwrap_or_else(|| "userdefined_t".to_string()),
                            PostgresType::List(inner) => match &inner.data_type {
                                PostgresType::UserDefined { .. } => format!(
                                    "{}[]",
                                    type_map
                                        .get(&(inner.as_ref() as *const _))
                                        .cloned()
                                        .unwrap_or_else(|| "userdefined_t".to_string())
                                ),
                                _ => format!("{}[]", inner.data_type.name().unwrap()),
                            },
                            _ => field.data_type.name().unwrap(),
                        };
                        (field_name, field_type)
                    })
                    .collect();
                types.push((type_name, field_ddls));
            } else if let PostgresType::List(inner) = &col.data_type {
                collect_types(inner, types, type_map); // Recursively collect list types
            }
        }

        let mut types = Vec::new();
        let mut type_map = std::collections::HashMap::new();
        for col in &self.columns {
            collect_types(col, &mut types, &mut type_map);
        }

        // Generate type DDLs
        let mut ddl = String::new();
        for (type_name, fields) in &types {
            let fields_ddl = fields
                .iter()
                .map(|(fname, ftype)| format!("\"{}\" {}", fname, ftype))
                .collect::<Vec<_>>()
                .join(", ");
            ddl.push_str(&format!("CREATE TYPE {} AS ({});\n", type_name, fields_ddl));
        }

        // Generate table DDL
        let cols = self
            .columns
            .iter()
            .map(|col| {
                let type_str = match &col.data_type {
                    PostgresType::UserDefined { .. } => type_map
                        .get(&(col as *const _))
                        .cloned()
                        .unwrap_or_else(|| "userdefined_t".to_string()),
                    PostgresType::List(inner) => match &inner.data_type {
                        PostgresType::UserDefined { .. } => format!(
                            "{}[]",
                            type_map
                                .get(&(inner.as_ref() as *const _))
                                .cloned()
                                .unwrap_or_else(|| "userdefined_t".to_string())
                        ),
                        _ => format!("{}[]", inner.data_type.name().unwrap()),
                    },
                    _ => col.data_type.name().unwrap(),
                };
                let nullability = if col.nullable { "" } else { " NOT NULL" };
                format!("\"{}\" {}{}", col.name, type_str, nullability)
            })
            .collect::<Vec<_>>()
            .join(", ");

        ddl.push_str(&format!("CREATE TEMP TABLE \"{}\" ({});", table_name, cols));
        ddl
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_schema_ddl() {
        let schema = PostgresSchema {
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: PostgresType::Int4,
                    nullable: false,
                },
                Column {
                    name: "data".to_string(),
                    data_type: PostgresType::UserDefined {
                        fields: vec![
                            Box::new(Column {
                                name: "field1".to_string(),
                                data_type: PostgresType::Text,
                                nullable: true,
                            }),
                            Box::new(Column {
                                name: "field2".to_string(),
                                data_type: PostgresType::Int8,
                                nullable: false,
                            }),
                        ],
                    },
                    nullable: true,
                },
            ],
        };

        let ddl = schema.ddl("test_table");

        let expected_ddl = r#"CREATE TYPE data_t AS ("f0" TEXT, "f1" INT8);
CREATE TEMP TABLE "test_table" ("id" INT4 NOT NULL, "data" data_t);"#;

        assert_eq!(ddl.trim(), expected_ddl.trim());
    }
}
