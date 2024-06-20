use anyhow::Result;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use thinp_userland::lua_bindings::*;

//-------------------------------------------------------------------------

fn main() -> Result<()> {
    // Set up tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    lua_main()?;

    Ok(())
}

//-------------------------------------------------------------------------
