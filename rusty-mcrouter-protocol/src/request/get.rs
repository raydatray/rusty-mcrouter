use crate::{errors::ParseError, key::Key};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetRequest {
    pub key: Key,

    // response projections requested from the backend
    pub return_value: bool,        // v
    pub return_client_flags: bool, // f
    pub return_cas: bool,          // c
    pub return_size: bool,         // s
    pub return_hit_state: bool,    // h
    pub return_last_access: bool,  // l

    // cache behavior
    pub check_cas: Option<u64>,    // C<n>
    pub override_cas: Option<u64>, // E<n>
    pub no_lru_bump: bool,         // u

    // order-sensitive N/T/t/R subset
    pub temporal: GetTemporalInstructions,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GetTemporalInstructions {
    instructions: [Option<GetTemporalInstruction>; 4],
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetTemporalInstruction {
    Vivify(i32),        // N<n>
    UpdateTtl(i32),     // T<n>
    ReturnTtl,          // t
    WinForRecache(i32), // R<n>
}

impl GetTemporalInstructions {
    pub(crate) fn push(&mut self, instruction: GetTemporalInstruction) -> Result<(), ParseError> {
        let slot = self
            .instructions
            .iter_mut()
            .find(|slot| slot.is_none())
            .ok_or(ParseError::TooManyFlags)?;

        *slot = Some(instruction);
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = &GetTemporalInstruction> {
        self.instructions.iter().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporal_instructions_preserve_insertion_order() {
        let mut instructions = GetTemporalInstructions::default();
        instructions
            .push(GetTemporalInstruction::Vivify(30))
            .unwrap();
        instructions
            .push(GetTemporalInstruction::UpdateTtl(60))
            .unwrap();
        instructions
            .push(GetTemporalInstruction::ReturnTtl)
            .unwrap();
        instructions
            .push(GetTemporalInstruction::WinForRecache(10))
            .unwrap();

        assert_eq!(
            instructions.iter().cloned().collect::<Vec<_>>(),
            vec![
                GetTemporalInstruction::Vivify(30),
                GetTemporalInstruction::UpdateTtl(60),
                GetTemporalInstruction::ReturnTtl,
                GetTemporalInstruction::WinForRecache(10),
            ]
        );
    }

    #[test]
    fn temporal_instructions_reject_more_than_four_entries() {
        let mut instructions = GetTemporalInstructions::default();
        for instruction in [
            GetTemporalInstruction::Vivify(30),
            GetTemporalInstruction::UpdateTtl(60),
            GetTemporalInstruction::ReturnTtl,
            GetTemporalInstruction::WinForRecache(10),
        ] {
            instructions.push(instruction).unwrap();
        }

        assert_eq!(
            instructions.push(GetTemporalInstruction::ReturnTtl),
            Err(ParseError::TooManyFlags)
        );
    }
}
