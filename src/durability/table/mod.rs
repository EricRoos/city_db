use super::{DurabilityError, Durable};

mod column_definition;
mod column_type;
mod table;

pub use column_definition::ColumnDefinition;
pub use column_type::ColumnType;
pub use table::{Page, Row, Table};

pub fn writeable_table_file(name: String) -> Result<std::fs::File, DurabilityError> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(name)
        .map_err(|e| DurabilityError::IoError(e))?;

    Ok(file)
}

pub fn create_table(name: String, columns: Vec<ColumnDefinition>) -> Result<(), String> {
    if table_exists(&name) {
        return Err(format!("Table {} already exists", name));
    }

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open(&name)
        .unwrap();

    let mut table = Table::new(name, columns);
    if let Err(e) = table.write_to_disk(&mut file) {
        return Err(format!("Error creating table: {:?}", e));
    }
    if table.add_page(&mut file).is_err() {
        return Err("Error adding page to table".to_string());
    }

    Ok(())
}

pub fn table_exists(name: &str) -> bool {
    std::path::Path::new(name).exists()
}

#[cfg(test)]
mod tests {
    use std::env;

    use table::Row;
    use tempfile::{env::temp_dir, tempdir};

    use super::*;

    #[test]
    fn test_read_write_on_disk() {
        let tmp_dir = tempdir();
        if let Err(e) = tmp_dir {
            panic!("Error creating temp dir: {:?}", e);
        }
        let tmp_dir = tmp_dir.unwrap();
        let temp_file_path = tmp_dir.path().join("test_table");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(temp_file_path)
            .unwrap();

        let mut table = Table::new(
            "test_table".to_string(),
            vec![
                ColumnDefinition::new("id".to_string(), ColumnType::Int, 11),
                ColumnDefinition::new("account_id".to_string(), ColumnType::Int, 11),
            ],
        );

        table.write_to_disk(&mut file).unwrap();
        if let Err(e) = table.add_page(&mut file) {
            panic!("Error adding page to table: {:?}", e);
        }

        let table = Table::read_from_disk(&mut file);

        if let Err(e) = table {
            panic!("Error reading table from disk: {:?}", e);
        }
        assert!(table.is_ok());

        let mut table = table.unwrap();
        assert!(table.column_count == 2);

        //add 3 rows
        for _ in 0..3 {
            let row_added = table.add_row(
                Row {
                    data: vec!["123".as_bytes().to_vec(), "1".as_bytes().to_vec()],
                },
                &mut file,
            );

            if let Err(e) = row_added {
                panic!("Error adding row to table: {:?}", e);
            }
        }

        let table = Table::read_from_disk(&mut file);
        if let Err(e) = table {
            panic!("Error reading table from disk: {:?}", e);
        }
        assert!(table.is_ok());
        let table = table.unwrap();
        assert!(table.row_count == 3);
        tmp_dir.close().unwrap();
    }
}
