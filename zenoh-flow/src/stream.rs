use async_trait::async_trait;

pub enum ZFFIFOError<T> {
    Full(T),
    Closed(T),
    Empty(T),
}

#[async_trait]
pub trait ZFLinkTrait<T> {
    fn try_send(&self, msg: T) -> Result<(), ZFFIFOError<T>>;

    async fn send(&self, msg: T) -> Result<(), ZFFIFOError<T>>;

    fn try_recv(&self) -> Result<(T, usize), ZFFIFOError<T>>;

    async fn recv(&self) -> Result<(T, usize), ZFFIFOError<T>>;

    /// Consumes the last message in the queue
    async fn take(&self) -> Result<(T, usize), ZFFIFOError<T>>;

    /// Same as take but not async, if no messages are present returns
    /// ZFFIFOError::Empty
    fn try_take(&self) -> Result<(T, usize), ZFFIFOError<T>>;

    /// Reads the last message in the queue without consuming it.
    /// The message will still be in the queue.
    async fn peek(&self) -> Result<(&T, usize), ZFFIFOError<T>>;

    /// Same as peek but not async, if no messages are present returns
    /// ZFFIFOError::Empty
    fn try_peek(&self) -> Result<(&T, usize), ZFFIFOError<T>>;

    fn is_closed(&self) -> bool;

    fn is_full(&self) -> bool;

    fn is_empty(&self) -> bool;

    fn len(&self) -> usize;

    fn capacity(&self) -> usize;

    fn id(&self) -> usize;
}
