use strum::Display;

#[derive(Display)]
pub enum CheckItem {
    CheckDatabaseConnection,
    CheckDatabaseVersionSupported,
    CheckAccountPermission,
    CheckIfDatabaseSupportCdc,
    CheckIfTableStructSupported,
}
