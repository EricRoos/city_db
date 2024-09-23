use super::ColumnType;

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
