use std::os::unix::fs::FileExt;

use memmap::Mmap;
use memmap::MmapOptions;

use crate::durability::Durable;

use super::ColumnDefinition;
use super::ColumnType;

const MAX_PAGE_SIZE: u64 = 4096;

pub struct Row {
    pub data: Vec<Vec<u8>>,
}

pub struct Table {
    pub name: [u8; 64],
    pub column_count: u32,
    pub columns: Vec<ColumnDefinition>,
    pub row_count: u128,
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

    pub fn page_size(&self) -> u64 {
        let row_size = self.row_size();
        if row_size < MAX_PAGE_SIZE {
            MAX_PAGE_SIZE
        } else {
            row_size
        }
    }

    pub fn page_count(&self) -> u128 {
        let row_size = self.row_size() as u128;
        let page_size = self.page_size() as u128;
        let row_count = self.row_count;

        row_size * row_count / page_size
    }

    pub fn add_page(&mut self, file: &mut std::fs::File) -> Result<(), String> {
        let page_size = self.page_size() as u128;
        let page_offset: u128 = (self.header_size() as u128) + ((self.page_count()) * page_size);

        let page_offset = page_offset.try_into();
        if let Err(e) = page_offset {
            return Err(format!("Error adding page to table: {:?}", e));
        }
        let page_offset: u64 = page_offset.unwrap();

        let page = vec![0; page_size as usize];
        if let Err(e) = file.write_all_at(&page, page_offset.try_into().unwrap()) {
            return Err(format!("Error adding page to table: {:?}", e));
        }

        Ok(())
    }

    pub fn page_at(&self, file: &std::fs::File, page: u64) -> Result<Mmap, String> {
        if page >= self.page_count() as u64 {
            return Err("Invalid page number".to_string());
        }
        let offset = self.header_size() + (page * self.page_size());

        let mmap = unsafe {
            MmapOptions::new()
                .len(self.page_size() as usize)
                .offset(offset)
                .map(file)
        };

        if let Err(e) = mmap {
            return Err(format!("Error mapping page to memory: {:?}", e));
        }

        Ok(mmap.unwrap())
    }

    pub fn row_size(&self) -> u64 {
        self.columns
            .iter()
            .fold(0, |acc, column| acc + column.length)
    }

    pub fn header_size(&self) -> u64 {
        68 + (self.column_count as u64 * ColumnDefinition::size()) + 16
    }

    pub fn last_page_at_limit(&self) -> bool {
        let row_size = self.row_size() as u128;
        let page_size = self.page_size() as u128;
        let row_count = self.row_count;

        (row_size * row_count) % page_size == 0
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

        if self.last_page_at_limit() {
            if self.add_page(file).is_err() {
                return Err("Error adding page to table".to_string());
            }
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
            let bytes = column.bytes();
            if let Err(e) = file.write_all_at(&bytes, offset) {
                return Err(super::DurabilityError::IoError(e));
            }
            offset += ColumnDefinition::size();
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
