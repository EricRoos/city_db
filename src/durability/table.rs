use std::{io::Read, os::unix::fs::FileExt};

use super::{DurabilityError, Durable};

const COLUMN_TYPE_INT: u32 = 1;
const COLUMN_TYPE_VARCHAR: u32 = 2;

pub enum ColumnType {
    Int,
    Varchar,
}

impl ColumnType {
    //function that returns the Bytes iterator for the column type
    pub fn bytes(&self) -> Vec<u8> {
        let code: u32 = self.into();
        code.to_ne_bytes().to_vec()
    }
}

impl Into<u32> for &ColumnType {
    fn into(self) -> u32 {
        match self {
            ColumnType::Int => COLUMN_TYPE_INT,
            ColumnType::Varchar => COLUMN_TYPE_VARCHAR,
        }
    }
}

pub struct ColumnDefinition {
    pub name: [u8; 64],
    pub column_type: ColumnType,
    pub length: u64,
}

impl ColumnDefinition {
    pub fn new(name: String, column_type: ColumnType, length: u64) -> Self {
        let name_bytes = name.as_bytes();
        let mut name_buffer = [0; 64];
        name_buffer[..name_bytes.len()].copy_from_slice(name_bytes);
        ColumnDefinition {
            name: name_buffer,
            column_type,
            length,
        }
    }

    pub fn size() -> u64 {
        76
    }

    pub fn bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        let column_type = &self.column_type;
        bytes.extend(self.name.iter());
        bytes.extend(column_type.bytes().iter());
        bytes.extend(self.length.to_ne_bytes().iter());
        bytes
    }
}

pub struct Row {
    pub data: Vec<Vec<u8>>,
}

pub struct Table {
    pub name: [u8; 64],
    pub column_count: u32,
    pub columns: Vec<ColumnDefinition>,
    pub row_count: u128,
}

pub fn table_exists(name: &str) -> bool {
    std::path::Path::new(name).exists()
}

impl Table {
    pub fn new(name: String, columns: Vec<ColumnDefinition>) -> Self {
        let name_bytes = name.as_bytes();
        let mut name_buffer = [0; 64];
        name_buffer[..name_bytes.len()].copy_from_slice(name_bytes);
        Table {
            name: name_buffer,
            column_count: columns.len() as u32,
            columns,
            row_count: 0,
        }
    }

    pub fn row_size(&self) -> u64 {
        self.columns
            .iter()
            .fold(0, |acc, column| acc + column.length)
    }

    pub fn header_size(&self) -> u64 {
        68 + (self.column_count as u64 * ColumnDefinition::size()) + 16
    }

    pub fn add_row(&mut self, row: Row, file: &mut std::fs::File) -> Result<(), String> {
        if row.data.len() != self.column_count as usize {
            return Err(format!(
                "Invalid row data expected {} columns got {} ",
                self.column_count,
                row.data.len()
            ));
        }

        let mut row_bytes: Vec<u8> = vec![];

        for (i, column) in self.columns.iter().enumerate() {
            if row.data[i].len() > column.length as usize {
                return Err("Invalid column data".to_string());
            }

            let resized_data = {
                let mut data = row.data[i].clone();
                data.resize(column.length as usize, 0);
                data
            };

            row_bytes.extend(resized_data.iter());
        }

        if row_bytes.len() != self.row_size() as usize {
            return Err(format!(
                "Invalid final row size got {}, expected {}",
                row_bytes.len(),
                self.row_size()
            ));
        }

        if let Err(e) = file.write_all_at(
            &row_bytes,
            self.header_size() + (self.row_size() * self.row_count as u64),
        ) {
            return Err(format!("Error writing row to disk: {:?}", e));
        }

        self.row_count += 1;
        if let Err(e) = self.write_row_count_to_disk(file) {
            return Err(format!("Error updating table row count: {:?}", e));
        }

        Ok(())
    }

    pub fn write_row_count_to_disk(&self, file: &mut std::fs::File) -> Result<(), String> {
        if let Err(e) = file.write_all_at(
            &self.row_count.to_ne_bytes(),
            68 + (self.column_count as u64 * ColumnDefinition::size()),
        ) {
            return Err(format!("Error writing row count to disk: {:?}", e));
        }

        Ok(())
    }
}

impl Durable for Table {
    fn write_to_disk(&mut self, file: &mut std::fs::File) -> Result<(), super::DurabilityError> {
        if let Err(e) = file.write_all_at(&self.name, 0) {
            return Err(super::DurabilityError::IoError(e));
        }

        if let Err(e) = file.write_all_at(&self.column_count.to_ne_bytes(), 64) {
            return Err(super::DurabilityError::IoError(e));
        }

        const COLUMN_DEFINITION_OFFSET: u64 = 68;
        let mut offset = COLUMN_DEFINITION_OFFSET;
        for column in &self.columns {
            let column_type: &ColumnType = &column.column_type;
            let column_type: u32 = column_type.into();
            let _ = file.write_all_at(&column.name, offset);
            offset += 64;
            let _ = file.write_all_at(&column_type.to_ne_bytes(), offset);
            offset += 4;
            let _ = file.write_all_at(&column.length.to_ne_bytes(), offset);
            offset += 8;
        }

        let _ = self.write_row_count_to_disk(file);
        Ok(())
    }

    fn read_from_disk(file: &mut std::fs::File) -> Result<Self, super::DurabilityError>
    where
        Self: Sized,
    {
        let mut name_buff: [u8; 64] = [0; 64];

        if let Err(e) = file.read_exact_at(&mut name_buff, 0) {
            return Err(super::DurabilityError::IoError(e));
        }

        let mut column_count_buff: [u8; 4] = [0; 4];
        if let Err(e) = file.read_exact_at(&mut column_count_buff, 64) {
            return Err(super::DurabilityError::IoError(e));
        }

        let column_count = u32::from_ne_bytes(column_count_buff);

        //read the column definitions
        let mut offset = 68;
        let mut columns = vec![];
        for _ in 0..column_count {
            let mut column_name_buff: [u8; 64] = [0; 64];
            if let Err(e) = file.read_exact_at(&mut column_name_buff, offset) {
                return Err(super::DurabilityError::IoError(e));
            }
            offset += 64;

            let mut column_type_buff: [u8; 4] = [0; 4];
            if let Err(e) = file.read_exact_at(&mut column_type_buff, offset) {
                return Err(super::DurabilityError::IoError(e));
            }
            offset += 4;
            println!("Column type: {:?}", column_type_buff);
            let column_type = match u32::from_ne_bytes(column_type_buff) {
                1 => ColumnType::Int,
                2 => ColumnType::Varchar,
                _ => {
                    return Err(super::DurabilityError::DbError(format!(
                        "Invalid column type: {}",
                        column_type_buff[0]
                    )))
                }
            };

            let mut column_length_buff: [u8; 8] = [0; 8];
            if let Err(e) = file.read_exact_at(&mut column_length_buff, offset) {
                return Err(super::DurabilityError::IoError(e));
            }
            offset += 8;

            let column_length = u64::from_ne_bytes(column_length_buff);

            columns.push(ColumnDefinition {
                name: column_name_buff,
                column_type,
                length: column_length,
            });
        }

        let row_count = {
            let mut row_count_buff: [u8; 16] = [0; 16];
            if let Err(e) = file.read_exact_at(&mut row_count_buff, offset) {
                return Err(super::DurabilityError::IoError(e));
            }
            u128::from_ne_bytes(row_count_buff)
        };

        Ok(Table {
            name: name_buff,
            column_count,
            columns,
            row_count,
        })
    }
}

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

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_write_on_disk() {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open("test_table")
            .unwrap();

        let mut table = Table::new(
            "test_table".to_string(),
            vec![
                ColumnDefinition::new("id".to_string(), ColumnType::Int, 11),
                ColumnDefinition::new("account_id".to_string(), ColumnType::Int, 11),
            ],
        );

        table.write_to_disk(&mut file).unwrap();

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
    }
}
