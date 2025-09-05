use super::{MemoryStorage, StoragePubSub};

impl StoragePubSub for MemoryStorage {
    fn subscribe(&mut self, token: mio::Token, channel: String) -> usize {
        self.add_subscriber(token, channel);
        self.get_subscriptions(token).len()
    }

    fn publish(&mut self, channel: String, message: String) -> usize {
        let subscribers = self.get_channel_subscriptions(channel.as_str());
        for token in subscribers.clone() {
            self.handle
                .send_message(token, channel.clone(), message.clone());
        }
        return subscribers.len();
    }

    fn unsubscribe(&mut self, token: mio::Token, channel: String) -> usize {
        self.remove_subscriber(token, channel);
        self.get_subscriptions(token).len()
    }
}
