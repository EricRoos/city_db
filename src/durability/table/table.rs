use std::os::unix::fs::FileExt;

use memmap::Mmap;
use memmap::MmapOptions;

use crate::durability::Durable;

use super::ColumnDefinition;
use super::ColumnType;

const MAX_PAGE_SIZE: u64 = 128;

#[derive(Debug)]
pub struct Row {
    pub data: Vec<Vec<u8>>,
}

pub struct Table {
    pub name: [u8; 64],
    pub column_count: u32,
    pub columns: Vec<ColumnDefinition>,
    pub row_count: u64,
}

pub struct Page {
    pub data: Mmap,
    pub page_number: u64,
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
            MAX_PAGE_SIZE - (MAX_PAGE_SIZE % row_size)
        } else {
            row_size
        }
    }

    pub fn page_data(&self, file: &std::fs::File, page: u64) -> Result<Vec<u8>, String> {
        let mmap = self.page_at(file, page);
        if let Err(e) = mmap {
            return Err(format!("Error getting page data: {:?}", e));
        }

        if let Err(e) = mmap {
            return Err(format!("Error getting page data: {:?}", e));
        }
        let mmap = mmap.unwrap();

        Ok(mmap.data.to_vec())
    }

    pub fn page_count(&self) -> u64 {
        let row_size = self.row_size();
        let page_size = self.page_size();
        let row_count = self.row_count;
        if row_count == 0 {
            return 0;
        }

        (row_size * row_count / page_size) + 1
    }

    pub fn add_page(&mut self, file: &mut std::fs::File) -> Result<(), String> {
        let page_size = self.page_size();
        let page_offset: u64 = match self.row_count == 0 {
            true => self.header_size(),
            false => (self.header_size()) + ((self.page_count()) * page_size),
        };

        let page = vec![0; page_size as usize];
        if let Err(e) = file.write_all_at(&page, page_offset.into()) {
            return Err(format!("Error adding page to table: {:?}", e));
        }

        Ok(())
    }

    pub fn page_at(&self, file: &std::fs::File, page: u64) -> Result<Page, String> {
        println!("Getting page at {} from disk", page);
        if page > self.page_count() {
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

        if let Err(e) = mmap {
            return Err(format!("Error mapping page to memory: {:?}", e));
        }
        let mmap = mmap.unwrap();

        Ok(Page {
            data: mmap,
            page_number: page,
        })
    }

    pub fn page_rows(&self, page: &Page) -> Vec<Row> {
        let mut rows = vec![];
        let row_size = self.row_size() as usize;
        let page_size = self.page_size() as usize;
        let rows_in_page = page_size / row_size;

        let row_count = if self.row_count as usize > rows_in_page {
            if page.page_number == self.page_count() - 1 {
                self.row_count as usize % rows_in_page
            } else {
                rows_in_page
            }
        } else {
            self.row_count as usize
        };

        for i in 0..row_count {
            let row_start = i * row_size;
            let row_end = row_start + row_size;
            let row_data = page.data[row_start..row_end].to_vec();
            let mut row = vec![];
            let mut j = 0;
            for column in self.columns.iter() {
                let column_start = column.length as usize * j;
                let column_end = column_start + column.length as usize;
                row.push(row_data[column_start..column_end].to_vec());
                j += 1;
            }
            rows.push(Row { data: row });
        }
        rows
    }

    pub fn row_size(&self) -> u64 {
        self.columns
            .iter()
            .fold(0, |acc, column| acc + column.length)
    }

    pub fn header_size(&self) -> u64 {
        68 + (self.column_count as u64 * ColumnDefinition::size()) + 8
    }

    pub fn last_page_at_limit(&self) -> bool {
        let row_size = self.row_size();
        let page_size = self.page_size();
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

        if self.last_page_at_limit() && self.add_page(file).is_err() {
            return Err("Error adding page to table".to_string());
        }

        if let Err(e) = file.write_all_at(
            &row_bytes,
            self.header_size() + (self.row_size() * self.row_count),
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

        let column_count_bytes = self.column_count.to_ne_bytes();
        if let Err(e) = file.write_all_at(&column_count_bytes, 64) {
            return Err(super::DurabilityError::IoError(e));
        }

        println!("Column count: {:?}", column_count_bytes);

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
        println!("Column count: {}", column_count);

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
            let mut row_count_buff: [u8; 8] = [0; 8];
            if let Err(e) = file.read_exact_at(&mut row_count_buff, offset) {
                return Err(super::DurabilityError::IoError(e));
            }
            u64::from_ne_bytes(row_count_buff)
        };

        Ok(Table {
            name: name_buff,
            column_count,
            columns,
            row_count,
        })
    }
}
