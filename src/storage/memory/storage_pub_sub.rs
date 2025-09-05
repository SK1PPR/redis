use super::{MemoryStorage, StoragePubSub};

impl StoragePubSub for MemoryStorage {
    fn subscribe(&mut self, token: mio::Token, channel: String) -> usize {
        self.add_subscriber(token, channel);
        self.get_subscriptions(token).len()
    }
}
