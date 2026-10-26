use gonvex_protocol::ReplicaCursor;

/// Resume only within the retained history of the current epoch.
pub(crate) fn cursor_resumable(
    client: Option<&ReplicaCursor>,
    current: &ReplicaCursor,
    retained: u64,
) -> bool {
    match client {
        None => false,
        Some(client) => {
            client.epoch == current.epoch
                && client.revision <= current.revision
                && client.revision >= retained
        }
    }
}
