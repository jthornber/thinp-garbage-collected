use crate::copier::base::*;

//-------------------------------------

struct FakeCopier {}

impl FakeCopier {
    pub fn new() -> Self {
        FakeCopier {}
    }
}

impl Copier for FakeCopier {
    fn copy(&mut self, ops: &[CopyOp]) -> Result<()> {
        Ok(())
    }

    fn zero(&mut self, ops: &[ZeroOp]) -> Result<()> {
        Ok(())
    }
}

//-------------------------------------
