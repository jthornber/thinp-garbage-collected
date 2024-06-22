use mlua::{Lua, Result, Table, UserData, UserDataMethods};
use std::path::PathBuf;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use crate::thin::mapping::*;
use crate::thin::*;
use crate::types::*;

//-------------------------------------------------------------------------

impl UserData for Mapping {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method("b", |_, this, ()| Ok(this.b));
        methods.add_method("e", |_, this, ()| Ok(this.e));
        methods.add_method("snap_time", |_, this, ()| Ok(this.snap_time));
    }
}

impl UserData for Pool {
    fn add_methods<'lua, M: UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method_mut("create_thin", |_, this, size: u64| {
            this.create_thin(size).map_err(mlua::Error::external)
        });
        methods.add_method_mut("create_thick", |_, this, size: u64| {
            this.create_thick(size).map_err(mlua::Error::external)
        });
        methods.add_method_mut("create_snap", |_, this, origin: ThinID| {
            this.create_snap(origin).map_err(mlua::Error::external)
        });
        methods.add_method_mut("delete_thin", |_, this, dev: ThinID| {
            this.delete_thin(dev).map_err(mlua::Error::external)
        });
        methods.add_method_mut(
            "get_write_mapping",
            |lua, this, (id, thin_begin, thin_end): (ThinID, VBlock, VBlock)| {
                let mappings = this
                    .get_write_mapping(id, thin_begin, thin_end)
                    .map_err(mlua::Error::external)?;
                let lua_mappings: Vec<Table> = mappings
                    .into_iter()
                    .map(|(vblock, mapping)| {
                        let table = lua.create_table().unwrap();
                        table.set("vblock", vblock).unwrap();
                        table
                            .set("mapping", lua.create_userdata(mapping).unwrap())
                            .unwrap();
                        table
                    })
                    .collect();
                Ok(lua_mappings)
            },
        );
        methods.add_method_mut(
            "discard",
            |_, this, (id, thin_begin, thin_end): (ThinID, VBlock, VBlock)| {
                this.discard(id, thin_begin, thin_end)
                    .map_err(mlua::Error::external)
            },
        );
    }
}

fn create_pool(dir: &str, nr_metadata_blocks: u64, nr_data_blocks: u64) -> Result<Pool> {
    let dir = PathBuf::from(dir);
    Pool::create(dir, nr_metadata_blocks, nr_data_blocks).map_err(mlua::Error::external)
}

fn register_pool_functions(lua: &Lua) -> Result<()> {
    let globals = lua.globals();
    // Register the create_pool function
    globals.set(
        "create_pool",
        lua.create_function(
            |_, (dir, nr_metadata_blocks, nr_data_blocks): (String, u64, u64)| {
                create_pool(&dir, nr_metadata_blocks, nr_data_blocks)
            },
        )?,
    )?;
    Ok(())
}

fn run_script(lua: &Lua, script_path: &str) -> Result<()> {
    // Read the Lua script from the file
    let script = std::fs::read_to_string(script_path).expect("Failed to read script");
    // Load and execute the Lua script
    lua.load(&script).exec()?;
    Ok(())
}

fn repl() -> Result<()> {
    let lua = Lua::new();
    // Register the Pool functions
    register_pool_functions(&lua)?;
    // Read the Lua REPL script from the file
    let repl_script = std::fs::read_to_string("lua/repl.lua").expect("Failed to read repl.lua");
    // Load and execute the Lua REPL script
    lua.load(&repl_script).exec()?;
    Ok(())
}

pub fn lua_main() -> Result<()> {
    // Parse command-line arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        // If a script file is provided, run the script
        let script_path = &args[1];
        let lua = Lua::new();
        register_pool_functions(&lua)?;
        run_script(&lua, script_path)?;
    } else {
        // If no script file is provided, enter the REPL mode
        repl()?;
    }
    Ok(())
}

//-------------------------------------------------------------------------
