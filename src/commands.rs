pub mod echo;
pub mod get;
pub mod ping;
pub mod rpush;
pub mod set;

/// The command trait.
pub trait Command {
    /// Gets the name of the comamnd.
    fn name(&self) -> String;

    /// Runs the command.
    async fn handle(
        &self,
        args: Vec<crate::resp::RespType>,
        store: &crate::store::SharedStore,
    ) -> crate::resp::RespType;
}
