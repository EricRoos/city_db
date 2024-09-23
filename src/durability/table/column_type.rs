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
