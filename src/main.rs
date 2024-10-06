use std::{
    borrow::{Borrow, BorrowMut},
    fs::File,
    io::{stdin, BufRead},
    str,
};

use durability::{
    table::{
        create_table, table_exists, writeable_table_file, ColumnDefinition, ColumnType, Row, Table,
    },
    Durable,
};
use query::{Query, QuerySource};

mod durability;
mod query;

fn stringify_result(row: &Row, column_defifnitions: &Vec<ColumnDefinition>) -> Vec<String> {
    let mut result = Vec::new();
    for (i, column) in row.data.iter().enumerate() {
        let column_type = column_defifnitions[i].column_type.borrow();
        match column_type {
            ColumnType::Int => {
                let mut buffer: Vec<u8> = vec![];
                for byte in column.iter() {
                    if *byte == 0 {
                        continue;
                    }
                    buffer.push(*byte);
                }
                let string = str::from_utf8(&buffer).unwrap();
                result.push(string.to_string());
            }
            _ => {
                result.push("invalid".to_string());
            }
        }
    }
    result
}

struct ResultSet {
    rows: Vec<Vec<String>>,
    execution_time: u128,
    execution_status: u8,
}

fn get_result_set(table: &Table, file: &File, query: Query) -> ResultSet {
    let mut result_rows: Vec<Vec<String>> = Vec::new();
    let start_time = std::time::Instant::now();
    let mut status: u8 = 0;
    match query {
        Query::Select(query_source, _scope) => match query_source {
            QuerySource::Table(_) => {
                for i in 0..table.page_count() {
                    let page = table.page_at(file, i).unwrap();
                    let rows = table.page_rows(&page);
                    for row in rows {
                        let result: Vec<String> = stringify_result(&row, &table.columns);
                        result_rows.push(result);
                    }
                }
                status = 1;
            }
            QuerySource::Invalid => {
                result_rows.push(vec!["Invalid query source".to_string()]);
            }
        },
    }
    let elapsed = start_time.elapsed();
    ResultSet {
        rows: result_rows,
        execution_time: elapsed.as_micros(),
        execution_status: status,
    }
}
fn prep_db() {
    if !table_exists("account_tble") {
        let _created = create_table(
            "account_tbl".to_string(),
            vec![
                ColumnDefinition::new("id".to_string(), ColumnType::Int, 11),
                ColumnDefinition::new("account_id".to_string(), ColumnType::Int, 11),
            ],
        );
    }
}

fn prep_table(file: &mut File) -> Table {
    let mut table = Table::read_from_disk(file).unwrap();
    let row = Row {
        data: vec!["10002".to_string().into(), "23".to_string().into()],
    };

    table.add_row(row, file).unwrap();
    table
}

fn execute_query(query: &String, table: &Table, file: &File) {
    let query: Query = query.into();
    let result_set = get_result_set(table, file, query);
    let result_set_size = result_set.rows.len();
    //for row in result_set.rows {
    //    println!("{:?}", row);
    //}

    println!(
        "Execution time: {:?}, Execution status: {:?}, Row(s) {:?}",
        result_set.execution_time, result_set.execution_status, result_set_size
    );
}

fn main() {
    prep_db();
    let mut file = writeable_table_file("account_tbl".to_string()).unwrap();
    let table = prep_table(&mut file);

    let mut buf_reader = std::io::BufReader::new(stdin());
    let mut buf = Vec::new();
    while buf_reader.read_until(b';', &mut buf).is_ok() {
        let ends_with_semi_colon = buf.ends_with(&[b';']);
        if !ends_with_semi_colon {
            continue;
        }
        let query = str::from_utf8(&buf).unwrap().to_string().trim().to_string();
        execute_query(&query, &table, &file);
        buf = Vec::new();
    }
}
