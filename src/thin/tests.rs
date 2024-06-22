#[cfg(test)]
mod tests {
    use crate::thin::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct PoolFixture {
        pool: Arc<Pool>,
        _temp_dir: TempDir,
    }

    impl PoolFixture {
        fn new(nr_metadata_blocks: u64, nr_data_blocks: u64) -> Result<Self> {
            let temp_dir = TempDir::new()?;
            let dir_path = temp_dir.path();

            // TempDir::new() has already created the directory, so we can directly call Pool::create
            let pool = Arc::new(Pool::create(dir_path, nr_metadata_blocks, nr_data_blocks)?);

            Ok(PoolFixture {
                pool,
                _temp_dir: temp_dir,
            })
        }

        fn pool(&self) -> Arc<Pool> {
            self.pool.clone()
        }
    }

    #[test]
    fn test_create_pool() -> Result<()> {
        let fixture = PoolFixture::new(1000, 10000)?;
        let pool = fixture.pool();

        // Assertions remain the same
        assert!(fixture._temp_dir.path().exists());
        assert!(fixture._temp_dir.path().join("node_file").exists());
        assert!(fixture._temp_dir.path().join("journal").exists());

        assert_eq!(pool.snap_time, 0);
        assert_eq!(pool.next_thin_id, 0);
        assert!(pool.active_devs.is_empty());

        Ok(())
    }
}
