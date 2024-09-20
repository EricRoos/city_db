use durability::{
    table::{
        create_table, table_exists, writeable_table_file, ColumnDefinition, ColumnType, Row, Table,
    },
    DatabaseConfig, Durable,
};

mod durability;

fn main() {
    if !table_exists("account_tble") {
        let created = create_table(
            "account_tbl".to_string(),
            vec![
                ColumnDefinition::new("id".to_string(), ColumnType::Int, 11),
                ColumnDefinition::new("account_id".to_string(), ColumnType::Int, 11),
            ],
        );
    }

    let file = writeable_table_file("account_tbl".to_string());
    if file.is_err() {
        panic!("Could not open table file");
    }
    let mut file = file.unwrap();
    let table = Table::read_from_disk(&mut file);
    if table.is_err() {
        panic!("Could not read table from disk");
    }
    let mut table = table.unwrap();
    println!("Current rows: {:?}", table.row_count);

    let row_added = table.add_row(
        Row {
            data: vec!["123".as_bytes().to_vec(), "1".as_bytes().to_vec()],
        },
        &mut file,
    );
    if row_added.is_err() {
        panic!("Could not add row to table");
    }
    let row_added = table.add_row(
        Row {
            data: vec!["123".as_bytes().to_vec(), "1".as_bytes().to_vec()],
        },
        &mut file,
    );
    if row_added.is_err() {
        panic!("Could not add row to table");
    }
    let row_added = table.add_row(
        Row {
            data: vec!["123".as_bytes().to_vec(), "1".as_bytes().to_vec()],
        },
        &mut file,
    );
    if row_added.is_err() {
        panic!("Could not add row to table");
    }
}
