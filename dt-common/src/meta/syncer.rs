use crate::meta::position::Position;

#[derive(Default)]
pub struct Syncer {
    pub received_position: Position,
    pub committed_position: Position,
}
