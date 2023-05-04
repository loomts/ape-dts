pub struct WorkDirUtil {}

impl WorkDirUtil {
    pub fn get_project_root() -> Option<String> {
        let mut project_root: Option<String> = None;

        match project_root::get_project_root() {
            Ok(pr) => project_root = Some(String::from(pr.to_str().unwrap())),
            Err(_) => {}
        }

        project_root
    }

    pub fn get_absolute_by_relative(relative_path: &str) -> Option<String> {
        let path;

        if !relative_path.starts_with("/") {
            path = format!("/{}", relative_path);
        } else {
            path = String::from(relative_path);
        }

        Some(format!(
            "{}{}",
            WorkDirUtil::get_project_root().unwrap(),
            path
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::WorkDirUtil;

    #[test]
    fn get_project_root_test() {
        let pr_option = WorkDirUtil::get_project_root();
        assert!(pr_option.is_some());
        println!("{}", pr_option.unwrap());
    }

    #[test]
    fn get_absolute_by_relative_test() {
        let mut path_option: Option<String>;
        path_option = WorkDirUtil::get_absolute_by_relative("fold/file.rs");
        assert!(path_option.is_some() && path_option.unwrap().ends_with("/fold/file.rs"));
        path_option = WorkDirUtil::get_absolute_by_relative("/fold/file.rs");
        assert!(path_option.is_some() && path_option.unwrap().ends_with("/fold/file.rs"));
    }
}
