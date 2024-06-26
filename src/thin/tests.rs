#[cfg(test)]
mod tests {
    use crate::thin::*;

    use anyhow::{ensure, Result};
    use std::sync::Arc;
    use tempfile::TempDir;

    struct Fixture {
        pool: Pool,
        _temp_dir: TempDir,
    }

    impl Fixture {
        fn new(nr_metadata_blocks: u64, nr_data_blocks: u64) -> Result<Self> {
            let temp_dir = TempDir::new()?;
            let dir_path = temp_dir.path();

            // TempDir::new() has already created the directory, so we can directly call Pool::create
            let pool = Pool::create(dir_path, nr_metadata_blocks, nr_data_blocks)?;

            Ok(Fixture {
                pool,
                _temp_dir: temp_dir,
            })
        }
    }

    #[test]
    fn test_create_pool() -> Result<()> {
        let fix = Fixture::new(1000, 10000)?;

        // Assertions remain the same
        assert!(fix._temp_dir.path().exists());
        assert!(fix._temp_dir.path().join("node_file").exists());
        assert!(fix._temp_dir.path().join("journal").exists());

        assert_eq!(fix.pool.snap_time, 0);
        assert_eq!(fix.pool.next_thin_id, 0);
        assert!(fix.pool.active_devs.is_empty());

        Ok(())
    }

    #[test]
    fn test_create_thin() -> Result<()> {
        let mut fix = Fixture::new(1000, 10000)?;
        fix.pool.create_thin(1000)?;
        Ok(())
    }

    #[test]
    fn test_create_thick() -> Result<()> {
        let mut fix = Fixture::new(1000, 10000)?;
        fix.pool.create_thick(1000)?;
        Ok(())
    }

    #[test]
    fn test_create_snap() -> Result<()> {
        let mut fix = Fixture::new(1000, 10000)?;
        let origin = fix.pool.create_thick(1000)?;
        let _snap = fix.pool.create_snap(origin)?;
        Ok(())
    }

    #[test]
    fn test_provision() -> Result<()> {
        let mut fix = Fixture::new(1000, 256_000_000)?;
        let dev = fix.pool.create_thin(1000)?;

        let mut thin = fix.pool.open_thin(dev);
        let mappings = fix.pool.get_read_mapping(&mut thin, 0, 1000)?;
        ensure!(mappings.is_empty());

        let mappings = fix.pool.get_write_mapping(&mut thin, 0, 1000)?;
        ensure!(!mappings.is_empty());
        eprintln!("mappings = {:?}", mappings);

        let mut total = 0;
        for (_vblock, m) in &mappings {
            total += m.len();
        }
        ensure!(total == 1000);

        let mappings = fix.pool.get_read_mapping(&mut thin, 0, 500)?;
        ensure!(!mappings.is_empty());

        Ok(())
    }
}
