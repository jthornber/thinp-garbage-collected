use mlua::{Lua, Result};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

//-------------------------------------------------------------------------

fn repl() -> Result<()> {
    let lua = Lua::new();

    // Read the Lua REPL script from the file
    let repl_script = std::fs::read_to_string("lua/repl.lua").expect("Failed to read repl.lua");

    // Load and execute the Lua REPL script
    lua.load(&repl_script).exec()?;

    Ok(())
}

fn main() -> Result<()> {
    // Set up tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    repl()
}

//-------------------------------------------------------------------------
