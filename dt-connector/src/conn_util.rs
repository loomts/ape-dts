#[macro_export(local_inner_macros)]
macro_rules! close_conn_pool {
    ($self:ident) => {
        if $self.conn_pool.is_closed() {
            Ok(())
        } else {
            Ok($self.conn_pool.close().await)
        }
    };
}
