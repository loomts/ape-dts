use strum::Display;

#[derive(Display)]
pub enum CheckItem {
    CheckDatabaseConnection,
    CheckDatabaseVersionSupported,
    CheckAccountPermission,
    CheckIfDatabaseSupportCdc,
    CheckIfStructExisted,
    CheckIfTableStructSupported,
}
