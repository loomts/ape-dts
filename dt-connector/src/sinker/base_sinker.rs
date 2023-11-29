pub struct BaseSinker {}

#[macro_export(local_inner_macros)]
macro_rules! call_batch_fn {
    ($self:ident, $data:ident, $batch_fn:expr) => {
        let all_count = $data.len();
        let mut sinked_count = 0;

        loop {
            let mut batch_size = $self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            $batch_fn($self, &mut $data, sinked_count, batch_size)
                .await
                .unwrap();

            sinked_count += batch_size;
            if sinked_count == all_count {
                break;
            }
        }
    };
}

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
