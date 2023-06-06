use crate::meta::common::database_model::StructModel;

pub struct DatabaseModelOps {}

impl DatabaseModelOps {
    pub fn check_diff(source_models: &Vec<StructModel>, sink_models: &Vec<StructModel>) -> bool {
        let mut check_result = true;

        if source_models.len() == 0 && sink_models.len() == 0 {
            println!("structs in source and sink is empty.");
            return check_result;
        }
        if source_models.len() == 0 {
            println!("structs in source is empty.");
            return false;
        } else if sink_models.len() == 0 {
            println!("structs in sink is empty.");
            return false;
        }

        // compare handler
        let mut compare_handler =
            |models1: &Vec<StructModel>, models2: &Vec<StructModel>, log_prefix: &str| {
                for model1 in models1 {
                    let mut is_match = false;
                    for model2 in models2 {
                        if model1 == model2 {
                            is_match = true;
                            break;
                        }
                    }
                    if !is_match {
                        println!("[{}] {}", log_prefix, model1.to_log_string());
                        check_result = false;
                    }
                }
            };

        compare_handler(source_models, sink_models, "source not existed in sink");
        compare_handler(sink_models, source_models, "sink not existed in source");

        check_result
    }
}
