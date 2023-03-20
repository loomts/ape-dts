use crate::error::Error;

pub struct DbTable {
    pub database_name: String,
    pub is_all: bool,
    pub table_name: String,
}

impl DbTable {
    pub fn from_str(str: &String, modes: &mut Vec<Self>) -> () {
        if str.is_empty() {
            return;
        }
        for db_table in str.split(",") {
            if db_table.contains(".") {
                let db_table_vec: Vec<&str> = db_table.split(".").collect();
                if db_table_vec.len() != 2 {
                    continue;
                }
                modes.push(Self {
                    database_name: String::from(db_table_vec[0]),
                    is_all: false,
                    table_name: String::from(db_table_vec[1]),
                })
            } else {
                modes.push(Self {
                    database_name: String::from(db_table),
                    is_all: true,
                    table_name: String::from(""),
                })
            }
        }
    }

    pub fn get_db_names(arr: &Vec<DbTable>) -> Result<Vec<String>, Error> {
        Ok(arr
            .iter()
            .filter(|x| !x.database_name.is_empty())
            .map(|x| String::from(x.database_name.as_str()))
            .collect())
    }

    pub fn get_tb_names(arr: &Vec<DbTable>) -> Result<(Vec<String>, Vec<String>), Error> {
        Ok((
            arr.iter()
                .filter(|x| !x.is_all && !x.table_name.is_empty())
                .map(|x| String::from(x.database_name.as_str()))
                .collect(),
            arr.iter()
                .filter(|x| !x.is_all && !x.table_name.is_empty())
                .map(|x| String::from(x.table_name.as_str()))
                .collect(),
        ))
    }

    // Returns: Vec<String, do_dbs's database names>, Vec<String, do_tbs's database_names>, Vec<String, do_tbs's table names>
    pub fn get_config_maps(
        arr: &Vec<DbTable>,
    ) -> Result<(Vec<String>, Vec<String>, Vec<String>), Error> {
        Ok((
            arr.iter()
                .filter(|x| !x.database_name.is_empty() && x.is_all)
                .map(|x| String::from(x.database_name.as_str()))
                .collect(),
            arr.iter()
                .filter(|x| !x.is_all && !x.table_name.is_empty())
                .map(|x| String::from(x.database_name.as_str()))
                .collect(),
            arr.iter()
                .filter(|x| !x.is_all && !x.table_name.is_empty())
                .map(|x| format!("{}.{}", x.database_name, x.table_name))
                .collect(),
        ))
    }
}
