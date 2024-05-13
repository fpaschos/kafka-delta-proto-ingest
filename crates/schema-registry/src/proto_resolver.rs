//!Directly taken and modified from: https://github.com/gklijs/schema_registry_converter/blob/main/src/proto_resolver.rs

use logos::Logos;
use crate::registry::SchemaRegistryError;

pub struct ProtoInfo {
    package: Option<String>,
    imports: Vec<String>,
}

impl ProtoInfo {
    #[inline]
    pub fn imports(&self) -> &[String] {
        self.imports.as_slice()
    }
}

/// Resolver that parses proto schema files and finds packages and imports.
pub struct ProtoResolver;

impl ProtoResolver {

    pub fn resolve(raw_schema: &str) -> Result<ProtoInfo, SchemaRegistryError> {
        let resolver = ResolverHelper::new(raw_schema);
        Ok(ProtoInfo {
            package: resolver.package,
            imports: resolver.imports,
        })
    }
}



#[derive(Logos, Debug, PartialEq)]
enum Token {
    #[regex(r"package\s+[a-zA-z0-9\\.\\_]+;")]
    Package,

    #[regex(r"message\s+[a-zA-z0-9\\_]+")]
    Message,

    #[regex(r#"import\s+"[a-zA-z0-9\\.\\_/]+";"#)]
    Import,

    #[token("{")]
    Open,

    #[token("}")]
    Close,

    #[regex(r"\S")]
    #[regex(r"[\s]+", logos::skip)]
    Ignorable,
}


/// Resolver helper implementation  that parses proto schema files and finds packages and imports.
///
/// Note: that this simplistic approach assumes "clean" protobuf representations that do not have comments
/// such as `//` or `/* */` in the schema file. Note that confluent schema registry api provides clean proto representations.
struct ResolverHelper {
    package: Option<String>,
    // indexes: Vec<Vec<i32>>,
    // names: Vec<String>,
    imports: Vec<String>,
}

impl ResolverHelper {
    pub fn new(s: &str) -> ResolverHelper {
        let mut package: Option<String> = None;
        // let mut indexes: Vec<Vec<i32>> = Vec::new();
        // let mut names: Vec<String> = Vec::new();
        let mut imports: Vec<String> = Vec::new();

        let mut lex = Token::lexer(s);
        let mut next: Option<Result<Token, _>> = lex.next();

        while let Some(token) = next {
            match token {
                Ok(Token::Package) => {
                    let slice = lex.slice();
                    package = Some(String::from(slice[8..slice.len() - 1].trim()));
                }
                Ok(Token::Import) => {
                    let slice = lex.slice();
                    imports.push(String::from(slice[8..slice.len() - 2].trim()));
                }
                _ => {}
            }
            next = lex.next();
        }

        ResolverHelper {
            package,
            // indexes,
            // names,
            imports,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    const PROTO_SAMPLE: &str = r#"
        syntax = "proto3";
        package model;

        import "google/protobuf/timestamp.proto";
        import "shared.proto";

        message Task {
            string id = 1;
            string created_by = 2;
            google.protobuf.Timestamp created_date = 3;
            Status status = 4;
        }
    "#;

    #[test]
    fn resolve_proto() {
        let info = ProtoResolver::resolve(PROTO_SAMPLE).unwrap();
        assert_eq!(info.package, Some("model".to_string()));
        assert_eq!(info.imports(), &["google/protobuf/timestamp.proto", "shared.proto"]);
    }
}
