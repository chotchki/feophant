//Implementation from here: https://hoverbear.org/blog/rust-state-machine-pattern/
pub struct SessionState<S> {
    state: S,
    user: String,
    database: String
}

// The following states can be the 'S' in StateMachine<S>

pub struct Start;

impl SessionState<Start> {
    pub fn new() -> Self {
        SessionState {
            user: "".to_string(),
            database: "".to_string(),
            state: Start {
            }
        }
    }
}

pub struct SSLRequest;

impl From<SessionState<Start>> for SessionState<SSLRequest> {
    fn from(val: SessionState<Start>) -> SessionState<SSLRequest> {
        SessionState {
            user: val.user,
            database: val.database,
            state: SSLRequest {
            }
        }
    }
}

pub struct Startup;

impl From<SessionState<Start>> for SessionState<Startup> {
    fn from(val: SessionState<Start>) -> SessionState<Startup> {
        SessionState {
            user: val.user,
            database: val.database,
            state: Startup {
            }
        }
    }
}

impl From<SessionState<SSLRequest>> for SessionState<Startup> {
    fn from(val: SessionState<SSLRequest>) -> SessionState<Startup> {
        SessionState {
            user: val.user,
            database: val.database,
            state: Startup {
            }
        }
    }
}

pub struct Done;

impl From<SessionState<Startup>> for SessionState<Done> {
    fn from(val: SessionState<Startup>) -> SessionState<Done> {
        SessionState {
            user: val.user,
            database: val.database,
            state: Done {
            }
        }
    }
}