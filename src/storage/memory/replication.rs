use crate::storage::file_utils::FileUtils;

use super::{MemoryStorage, Replication};

impl Replication for MemoryStorage {
    fn add_replication_client(&mut self, token: mio::Token) {
        self.replication_clients.insert(token);
    }

    fn send_file(&self, token: mio::Token) {
        if self.replication_clients.contains(&token) {
            let contents = FileUtils::get_db_as_file();
            self.handle.send_file(token, contents);
        }
    }
}
