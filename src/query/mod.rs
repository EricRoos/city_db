use std::{
    fs::File,
    io::{BufRead, BufReader, Bytes, Read},
};

#[derive(Debug)]
pub enum Scope {
    All,
}

#[derive(Debug)]
pub enum QuerySource {
    Table(String),
    IntoTable(String),
    Invalid,
}

#[derive(Debug)]
pub enum ColumnList {
    Columns(Vec<String>),
    Invalid,
}

#[derive(Debug)]
pub enum ValueList {
    Values(Vec<Vec<Vec<u8>>>),
    Invalid,
}

impl From<&mut Vec<u8>> for ColumnList {
    fn from(query: &mut Vec<u8>) -> Self {
        let columns = pop_string_inside_parenthesis(query);
        let columns = columns.split(',').map(|s| s.trim().to_string()).collect();
        let trailing_space = query.remove(0);
        if trailing_space != b' ' {
            return ColumnList::Invalid;
        }
        ColumnList::Columns(columns)
    }
}

#[derive(Debug)]
pub enum Query {
    Select(QuerySource, Scope),
    Insert(QuerySource, ColumnList, ValueList),
}

impl From<&mut Vec<u8>> for ValueList {
    fn from(query: &mut Vec<u8>) -> Self {
        const VALUES_TOKEN: &str = "VALUES";
        let token = pop_word(query);
        if token != VALUES_TOKEN {
            return ValueList::Invalid;
        }
        let mut rows = vec![];
        let mut value_string = pop_string_inside_parenthesis(query);
        while !value_string.is_empty() {
            let columns: Vec<String> = value_string
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();

            let columns: Vec<Vec<u8>> = columns.iter().map(|s| s.as_bytes().to_vec()).collect();
            rows.push(columns);
            value_string = pop_string_inside_parenthesis(query);
        }
        ValueList::Values(rows)
    }
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

fn pop_string_inside_parenthesis(query: &mut Vec<u8>) -> String {
    let mut word = String::new();
    while let Some(&c) = query.first() {
        query.remove(0);
        if c == b')' {
            break;
        }

        if c != b'(' {
            word.push(c as char);
        }
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
            "INTO" => {
                let table = pop_word(query);
                QuerySource::IntoTable(table)
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

fn read_word<R: Read>(reader: &mut BufReader<R>) -> String {
    let mut buf = vec![];
    let _ = reader.read_until(b' ', &mut buf);
    std::str::from_utf8(&buf).unwrap().to_string()
}

impl<R: Read> From<&mut BufReader<R>> for Query {
    fn from(value: &mut BufReader<R>) -> Self {
        read_word(value);
        todo!()
    }
}

impl From<&mut Vec<u8>> for Query {
    fn from(query: &mut Vec<u8>) -> Self {
        const SELECT: &str = "SELECT";
        const INSERT: &str = "INSERT";

        let word = pop_word(query);
        match word.as_str() {
            SELECT => {
                let query_source = QuerySource::from(query);
                Query::Select(query_source, Scope::All)
            }
            INSERT => {
                let query_source: QuerySource = query.into();
                let column_list: ColumnList = query.into();
                let data: ValueList = query.into();
                Query::Insert(query_source, column_list, data)
            }
            _ => panic!("Invalid query"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        borrow::{Borrow, BorrowMut},
        io::BufReader,
    };

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
    fn parse_select_query() {
        let query: Query = "SELECT FROM users".into();
        match query {
            Query::Select(query_source, _scope) => match query_source {
                QuerySource::Table(table) => {
                    assert_eq!(table, "users");
                }
                _ => {
                    panic!("Invalid query source");
                }
            },
            _ => {
                panic!("Invalid query");
            }
        }
    }

    #[test]
    fn parse_insert_query() {
        let query: Query = "INSERT INTO users (id, account_id) VALUES (1,2) (3,4)".into();
        println!("{:?}", query);
        match query {
            Query::Insert(query_source, column_list, data) => {
                match query_source {
                    QuerySource::IntoTable(table) => {
                        assert_eq!(table, "users");
                    }
                    _ => {
                        panic!("Invalid query source");
                    }
                }
                match column_list {
                    super::ColumnList::Columns(columns) => {
                        assert_eq!(columns, vec!["id", "account_id"]);
                    }
                    _ => {
                        panic!("Invalid columns");
                    }
                }
                match data {
                    super::ValueList::Values(data) => {
                        let expected: Vec<Vec<Vec<u8>>> = vec![
                            vec!["1".as_bytes().to_vec(), "2".as_bytes().to_vec()],
                            vec!["6".as_bytes().to_vec(), "4".as_bytes().to_vec()],
                        ];
                        assert_eq!(data, expected)
                    }
                    _ => {
                        panic!("Invalid data");
                    }
                }
            }
            _ => {
                panic!("Invalid query");
            }
        }
    }

    #[test]
    fn test_read_word_bufreader() {
        let data: &[u8] = "abcdef".as_bytes();
        let mut buf_reader = BufReader::new(data);
        crate::query::read_word(&mut buf_reader);
        assert_eq!(word, "SELECT".as_bytes());
    }
}
