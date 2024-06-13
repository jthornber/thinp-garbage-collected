use anyhow::Result;
use std::env;
use std::path::Path;
use thinp_userland::journal::*;

//-------------------------------------------------------------------------

fn dump<P: AsRef<Path>>(p: P) -> Result<()> {
    let mut journal = Journal::open(p, false)?;
    journal.dump(&mut std::io::stdout())?;
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <path_to_journal>", args[0]);
        std::process::exit(1);
    }

    let path = &args[1];
    if let Err(e) = dump(path) {
        eprintln!("Error dumping journal: {:?}", e);
        std::process::exit(1);
    }
    Ok(())
}

//-------------------------------------------------------------------------
