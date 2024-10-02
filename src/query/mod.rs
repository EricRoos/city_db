pub enum Scope {
    All,
}
pub enum QuerySource {
    Table(String),
    Invalid,
}

pub enum Query {
    Select(QuerySource, Scope),
}

fn pop_word(query: &mut Vec<u8>) -> String {
    let mut word = String::new();
    while let Some(&c) = query.first() {
        if c == b' ' {
            query.remove(0);
            break;
        }
        word.push(c as char);
        query.remove(0);
    }
    word
}

impl From<&mut Vec<u8>> for QuerySource {
    fn from(query: &mut Vec<u8>) -> Self {
        let word = pop_word(query);
        match word.as_str() {
            "FROM" => {
                let table = pop_word(query);
                QuerySource::Table(table)
            }
            _ => QuerySource::Invalid,
        }
    }
}

impl From<&str> for Query {
    fn from(query: &str) -> Self {
        let mut query = query.as_bytes().to_vec();
        Query::from(&mut query)
    }
}

impl From<&String> for Query {
    fn from(query: &String) -> Self {
        let mut query = query.as_bytes().to_vec();
        Query::from(&mut query)
    }
}

impl From<&mut Vec<u8>> for Query {
    fn from(query: &mut Vec<u8>) -> Self {
        const SELECT: &str = "SELECT";
        let word = pop_word(query);
        match word.as_str() {
            SELECT => {
                let query_source = QuerySource::from(query);
                Query::Select(query_source, Scope::All)
            }
            _ => panic!("Invalid query"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::{Borrow, BorrowMut};

    use super::{Query, QuerySource};

    #[test]
    fn test_pop_word() {
        let mut query = "SELECT * FROM users".as_bytes().to_vec();
        let word = super::pop_word(query.borrow_mut());
        assert_eq!(word, "SELECT");
    }

    #[test]
    fn parse_query_source() {
        let query_str = "FROM users";
        let mut query = query_str.as_bytes().to_vec();
        let query_source = QuerySource::from(query.borrow_mut());
        match query_source {
            QuerySource::Table(table) => {
                assert_eq!(table, "users");
            }
            _ => {
                panic!("Invalid query source");
            }
        }
    }

    #[test]
    fn parse_query() {
        let query: Query = "SELECT FROM users".into();
        match query {
            Query::Select(query_source, scope) => match query_source {
                QuerySource::Table(table) => {
                    assert_eq!(table, "users");
                }
                _ => {
                    panic!("Invalid query source");
                }
            },
        }
    }
}
