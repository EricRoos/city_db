use std::{io::Read, os::unix::fs::FileExt};

use database::{DatabaseFile, DatabaseFileHeader};

pub mod database;
pub mod table;

pub trait Durable {
    fn write_to_disk(&mut self, file: &mut std::fs::File) -> Result<(), DurabilityError>;
    fn read_from_disk(file: &mut std::fs::File) -> Result<Self, DurabilityError>
    where
        Self: Sized;
}

#[derive(Debug)]
pub enum DurabilityError {
    IoError(std::io::Error),
    DbError(String),
}

pub struct DatabaseConfig {
    pub name: String,
}

fn database_exists(name: &str) -> bool {
    std::path::Path::new(name).exists()
}
fn write_to_disk(database: &DatabaseConfig) -> Result<(), DurabilityError> {
    let file = std::fs::File::create(&database.name);
    if let Err(e) = file {
        return Err(DurabilityError::IoError(e));
    }

    let mut file = file.unwrap();

    let name_bytes = database.name.as_bytes();
    let mut name: [u8; 64] = [0; 64];
    name[..name_bytes.len()].copy_from_slice(name_bytes);

    let mut database = DatabaseFile {
        header: DatabaseFileHeader {
            name,
            table_count: 0,
        },
    };

    if let Err(e) = database.write_to_disk(&mut file) {
        return Err(e);
    }

    Ok(())
}

pub fn init_db(database: &DatabaseConfig) -> Result<(), DurabilityError> {
    if database_exists(&database.name) {
        return Err(DurabilityError::DbError(
            "DatabaseConfig already exists".to_string(),
        ));
    }

    write_to_disk(&database)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_db() {
        let name = String::from("test");
        let result = init_db(&DatabaseConfig { name });
        assert!(result.is_ok());

        let mut file = std::fs::File::open("test".to_string()).unwrap();
        let header = DatabaseFileHeader::read_from_disk(&mut file);
        assert!(header.is_ok());
        let header = header.unwrap();
        assert_eq!(
            std::str::from_utf8(&header.name).unwrap(),
            format!("{:\0<64}", "test")
        );
        assert_eq!(0, header.table_count);
    }
}
