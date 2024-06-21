use crate::copier::base::*;

//-------------------------------------

pub struct FakeCopier {}

impl FakeCopier {
    pub fn new() -> Self {
        FakeCopier {}
    }
}

impl Copier for FakeCopier {
    fn exec(&self, ops: &[DataOp]) -> Result<()> {
        Ok(())
    }
}

//-------------------------------------
