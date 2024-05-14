use rand::prelude::*;
use rand_chacha::ChaCha20Rng;

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

//-------------------------------------------------------------------------

/// Manages a set of active scope ids.  Used by ReferenceContext::Scoped.
pub struct ScopeRegister {
    rng: ChaCha20Rng,
    active_scopes: BTreeSet<u32>,
}

impl Default for ScopeRegister {
    fn default() -> Self {
        ScopeRegister {
            rng: ChaCha20Rng::from_seed(Default::default()),
            active_scopes: BTreeSet::new(),
        }
    }
}

pub struct ScopeProxy {
    register: Arc<Mutex<ScopeRegister>>,
    pub id: u32,
}

impl Drop for ScopeProxy {
    fn drop(&mut self) {
        self.register.lock().unwrap().drop_scope(self.id);
    }
}

impl ScopeRegister {
    fn find_unused_id(&mut self) -> u32 {
        for _ in 0..100 {
            let id = self.rng.next_u32();
            if !self.active_scopes.contains(&id) {
                return id;
            }
        }

        panic!("something wrong in scope register");
    }

    fn drop_scope(&mut self, id: u32) {
        self.active_scopes.remove(&id);
    }
}

pub fn new_scope(register: Arc<Mutex<ScopeRegister>>) -> ScopeProxy {
    let mut reg = register.lock().unwrap();
    let id = reg.find_unused_id();
    reg.active_scopes.insert(id);
    ScopeProxy {
        register: register.clone(),
        id,
    }
}

//-------------------------------------------------------------------------
