use std::{io::Read, os::unix::fs::FileExt};

use super::{DurabilityError, Durable};

pub struct DatabaseFile {
    pub header: DatabaseFileHeader,
}

impl Durable for DatabaseFile {
    fn write_to_disk(&mut self, file: &mut std::fs::File) -> Result<(), DurabilityError> {
        self.header.write_to_disk(file)
    }

    fn read_from_disk(file: &mut std::fs::File) -> Result<Self, DurabilityError>
    where
        Self: Sized,
    {
        let header = DatabaseFileHeader::read_from_disk(file)?;
        Ok(DatabaseFile { header })
    }
}

pub struct DatabaseFileHeader {
    pub name: [u8; 64],
    pub table_count: u32,
}

impl Durable for DatabaseFileHeader {
    fn write_to_disk(&mut self, file: &mut std::fs::File) -> Result<(), DurabilityError> {
        let bytes_written = file.write_at(&self.name, 0);
        if bytes_written.unwrap() != 64 {
            return Err(DurabilityError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to write header name",
            )));
        }

        let bytes_written = file.write_at(&self.table_count.to_ne_bytes(), 64);
        if bytes_written.unwrap() != 4 {
            return Err(DurabilityError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to write header column count",
            )));
        }

        Ok(())
    }

    fn read_from_disk(file: &mut std::fs::File) -> Result<Self, DurabilityError>
    where
        Self: Sized,
    {
        const name_size: usize = 64;
        const count_size: usize = 4;
        const header_size: usize = name_size + count_size;

        let mut header_buffer = [0; header_size];
        file.read_exact(&mut header_buffer).unwrap();

        let mut name = [0; name_size];
        name.copy_from_slice(&header_buffer[..name_size]);

        let table_count = u32::from_ne_bytes(
            header_buffer[name_size..name_size + count_size]
                .try_into()
                .unwrap(),
        );

        Ok(DatabaseFileHeader { name, table_count })
    }
}
