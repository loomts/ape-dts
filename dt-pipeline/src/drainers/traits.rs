use dt_common::error::Error;
use dt_meta::{ddl_data::DdlData, dt_data::DtData, row_data::RowData};

#[allow(clippy::type_complexity)]
pub trait DataDrainer {
    fn drain_dmls(
        &mut self,
        mut datas: Vec<DtData>,
    ) -> Result<(Vec<RowData>, Option<String>, Option<String>), Error> {
        let mut dml_data = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in datas.drain(..) {
            match i {
                DtData::Commit { position, .. } => {
                    last_commit_position = Some(position);
                    last_received_position = last_commit_position.clone();
                    continue;
                }

                DtData::Dml { row_data } => {
                    last_received_position = Some(row_data.position.clone());
                    dml_data.push(row_data);
                }

                _ => {}
            }
        }

        Ok((dml_data, last_received_position, last_commit_position))
    }

    fn drain_ddl(
        &mut self,
        mut data: Vec<DtData>,
    ) -> Result<(Vec<DdlData>, Option<String>, Option<String>), Error> {
        let mut result = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.drain(..) {
            match i {
                DtData::Commit { position, .. } => {
                    last_commit_position = Some(position);
                    last_received_position = last_commit_position.clone();
                    continue;
                }

                DtData::Ddl { ddl_data } => {
                    result.push(ddl_data);
                }

                _ => {}
            }
        }

        Ok((result, last_received_position, last_commit_position))
    }

    fn drain_raw(
        &mut self,
        mut data: Vec<DtData>,
    ) -> Result<(Vec<DtData>, Option<String>, Option<String>), Error> {
        let mut raw_data = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.drain(..) {
            match &i {
                DtData::Commit { position, .. } => {
                    last_commit_position = Some(position.to_string());
                    last_received_position = last_commit_position.clone();
                    continue;
                }

                DtData::Redis { entry } => {
                    last_received_position = Some(entry.position.to_string());
                    last_commit_position = last_received_position.clone();
                    if !entry.is_raw() && entry.cmd.get_name().eq_ignore_ascii_case("ping") {
                        continue;
                    }
                    raw_data.push(i);
                }

                DtData::Dml { row_data } => {
                    last_received_position = Some(row_data.position.clone());
                    raw_data.push(i);
                }

                _ => {
                    raw_data.push(i);
                }
            }
        }

        Ok((raw_data, last_received_position, last_commit_position))
    }
}
