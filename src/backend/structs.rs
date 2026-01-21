pub enum TableColumnTypeMode {
    PRIMITIVE,
    ADHOC_SINGLE_SELECT,
    ADHOC_MULTIPLE_SELECT,
    REFERENCE,
    CHILD_OBJECT,
    CHILD_TABLE
}

pub struct TableColumnType {
    id: i64,
    mode: TableColumnTypeMode
}

pub struct TableColumn {
    id: i64,
    name: String,
    column_type: TableColumnType,
    column_width: i64,
    column_ordering: i64,
    is_nullable: bool,
    is_unique: bool,
    is_primary_key: bool,
    is_surrogate_key: bool
}

pub struct Table {
    id: i64,
    parent_table_id: i64,
    name: String,
    columns: Vec<TableColumn>
}