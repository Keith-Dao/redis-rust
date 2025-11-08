//! This module contains the basis of commands.

use std::sync::Arc;

use tokio::sync::RwLock;

pub mod echo;
pub mod get;
pub mod ping;
pub mod rpush;
pub mod set;

#[async_trait::async_trait]
/// The command trait.
pub trait Command: Send + Sync {
    /// Gets the name of the comamnd.
    fn name(&self) -> String;

    /// Runs the command.
    async fn handle(
        &self,
        args: Vec<crate::resp::RespType>,
        store: &crate::store::SharedStore,
    ) -> crate::resp::RespType;
}

/// A command register.
pub struct Register(std::collections::HashMap<String, Box<dyn Command>>);

impl Register {
    /// An empty command register.
    pub fn new() -> Self {
        Self(std::collections::HashMap::new())
    }

    /// Registers one command.
    pub fn register(&mut self, command: Box<dyn Command>) {
        self.0.insert(command.name().to_uppercase(), command);
    }

    /// Registers multiple commands.
    pub fn register_multiple(&mut self, commands: Vec<Box<dyn Command>>) {
        for command in commands {
            self.register(command);
        }
    }

    /// Handles the command.
    pub async fn handle(
        &self,
        command: String,
        args: Vec<crate::resp::RespType>,
        store: &crate::store::SharedStore,
    ) -> crate::resp::RespType {
        match self.0.get(&command.to_uppercase()) {
            Some(command) => command.handle(args, store).await,
            _ => {
                crate::resp::RespType::SimpleError(format!("ERR Command ({command}) is not valid"))
            }
        }
    }
}

impl PartialEq for Register {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }

        self.0.iter().all(|(key, command)| match other.0.get(key) {
            Some(other_command) if command.name() == other_command.name() => true,
            _ => false,
        })
    }
}

impl std::fmt::Debug for Register {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut commands = self
            .0
            .values()
            .map(|command| command.name())
            .collect::<Vec<_>>();
        commands.sort_unstable();

        fmt.debug_struct("Register")
            .field("Commands", &commands)
            .finish()
    }
}

pub type SharedRegister = Arc<RwLock<Register>>;

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};

    // --- Mock commands ---
    pub trait CloneableCommand: Command {
        /// Clones the command into a box.
        fn clone_box(&self) -> Box<dyn CloneableCommand>;
    }

    impl<T> CloneableCommand for T
    where
        T: Command + Clone + 'static,
    {
        fn clone_box(&self) -> Box<dyn CloneableCommand> {
            Box::new(self.clone())
        }
    }

    impl Clone for Box<dyn CloneableCommand> {
        fn clone(&self) -> Self {
            self.clone_box()
        }
    }

    #[derive(Clone)]
    struct A;

    #[async_trait::async_trait]
    impl Command for A {
        fn name(&self) -> String {
            "A".into()
        }

        async fn handle(
            &self,
            _: Vec<crate::resp::RespType>,
            _: &crate::store::SharedStore,
        ) -> crate::resp::RespType {
            crate::resp::RespType::SimpleString("A".into())
        }
    }

    #[derive(Clone)]
    struct B;

    #[async_trait::async_trait]
    impl Command for B {
        fn name(&self) -> String {
            "B".into()
        }

        async fn handle(
            &self,
            _: Vec<crate::resp::RespType>,
            _: &crate::store::SharedStore,
        ) -> crate::resp::RespType {
            crate::resp::RespType::SimpleString("B".into())
        }
    }

    // --- Fixtures ---
    #[fixture]
    fn store() -> crate::store::SharedStore {
        crate::store::new()
    }

    // --- Tests ---
    #[rstest]
    fn test_new() {
        let expected = Register(std::collections::HashMap::new());
        assert_eq!(expected, Register::new());
    }

    #[rstest]
    fn test_register() {
        let mut expected = Register(std::collections::HashMap::new());
        expected.0.insert("A".into(), Box::new(A));
        let mut result = Register::new();
        result.register(Box::new(A));
        assert_eq!(expected, result);
    }

    #[rstest]
    #[case::single(vec![("A", Box::new(A) as Box<dyn CloneableCommand>)])]
    #[case::multiple(vec![("A", Box::new(A) as Box<dyn CloneableCommand>), ("B", Box::new(B) as Box<dyn CloneableCommand>)])]
    fn test_register_multiple(#[case] commands: Vec<(&str, Box<dyn CloneableCommand>)>) {
        let expected = Register(
            commands
                .iter()
                .map(|(name, command)| (name.to_string(), command.clone() as Box<dyn Command>))
                .collect(),
        );
        let mut result = Register::new();
        result.register_multiple(
            commands
                .into_iter()
                .map(|(_, command)| command as Box<dyn Command>)
                .collect(),
        );
        assert_eq!(expected, result);
    }

    #[rstest]
    #[case::a("A", crate::resp::RespType::SimpleString("A".into()))]
    #[case::b("B", crate::resp::RespType::SimpleString("B".into()))]
    #[tokio::test]
    async fn test_handle(
        store: crate::store::SharedStore,
        #[case] command: String,
        #[case] expected: crate::resp::RespType,
    ) {
        let mut register = Register::new();
        register.register_multiple(vec![Box::new(A), Box::new(B)]);
        assert_eq!(expected, register.handle(command, vec![], &store).await);
    }

    #[rstest]
    #[case::single(vec![Box::new(A) as Box<dyn CloneableCommand>], "Register { Commands: [\"A\"] }")]
    #[case::multiple(vec![Box::new(A) as Box<dyn CloneableCommand>, Box::new(B) as Box<dyn CloneableCommand>], "Register { Commands: [\"A\", \"B\"] }")]
    fn test_fmt(#[case] commands: Vec<Box<dyn CloneableCommand>>, #[case] expected: &str) {
        let register = Register(
            commands
                .into_iter()
                .map(|command| (command.name(), command as Box<dyn Command>))
                .collect(),
        );
        assert_eq!(expected, format!("{:?}", register));
    }

    #[rstest]
    #[case::length(
        Register(vec![("A".to_string(), Box::new(A) as Box<dyn Command>)].into_iter().collect()),
        Register(vec![("A".to_string(), Box::new(A) as Box<dyn Command>), ("B".to_string(), Box::new(B) as Box<dyn Command>)].into_iter().collect())
    )]
    #[case::mismatch_keys(
        Register(vec![("A".to_string(), Box::new(A) as Box<dyn Command>)].into_iter().collect()),
        Register(vec![("B".to_string(), Box::new(A) as Box<dyn Command>)].into_iter().collect())
    )]
    #[case::mismatch_values(
        Register(vec![("A".to_string(), Box::new(A) as Box<dyn Command>)].into_iter().collect()),
        Register(vec![("A".to_string(), Box::new(B) as Box<dyn Command>)].into_iter().collect())
    )]
    fn test_register_equal(#[case] a: Register, #[case] b: Register) {
        assert_ne!(a, b);
    }
}
