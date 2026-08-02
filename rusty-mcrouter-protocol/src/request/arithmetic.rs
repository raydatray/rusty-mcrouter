use crate::{errors::ParseError, key::Key};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArithmeticRequest {
    pub key: Key,

    // response projections requested from the backend
    pub return_value: bool, // v
    pub return_cas: bool,   // c

    // cache behavior
    pub mode: ArithmeticMode,
    pub delta: u64,                 // D<n>, default 1
    pub initial_value: Option<u64>, // J<n>, requires N
    pub compare_cas: Option<u64>,   // C<n>
    pub override_cas: Option<u64>,  // E<n>

    // order-sensitive N/T/t subset
    pub temporal: ArithmeticTemporalInstructions,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArithmeticMode {
    Increment, // M<I> or M<+>, the default
    Decrement, // M<D> or M<->
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArithmeticTemporalInstructions {
    instructions: [Option<ArithmeticTemporalInstruction>; 3],
    len: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArithmeticTemporalInstruction {
    Vivify(i32),    // N<n>
    UpdateTtl(i32), // T<n>
    ReturnTtl,      // t
}

impl ArithmeticTemporalInstructions {
    pub(crate) fn push(
        &mut self,
        instruction: ArithmeticTemporalInstruction,
    ) -> Result<(), ParseError> {
        let slot = self
            .instructions
            .get_mut(usize::from(self.len))
            .ok_or(ParseError::TooManyFlags)?;
        *slot = Some(instruction);
        self.len += 1;
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = &ArithmeticTemporalInstruction> {
        self.instructions[..usize::from(self.len)]
            .iter()
            .map(|instruction| instruction.as_ref().expect("dense temporal instructions"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporal_instructions_preserve_insertion_order() {
        let mut instructions = ArithmeticTemporalInstructions::default();
        instructions
            .push(ArithmeticTemporalInstruction::Vivify(30))
            .unwrap();
        instructions
            .push(ArithmeticTemporalInstruction::UpdateTtl(60))
            .unwrap();
        instructions
            .push(ArithmeticTemporalInstruction::ReturnTtl)
            .unwrap();

        assert_eq!(
            instructions.iter().cloned().collect::<Vec<_>>(),
            vec![
                ArithmeticTemporalInstruction::Vivify(30),
                ArithmeticTemporalInstruction::UpdateTtl(60),
                ArithmeticTemporalInstruction::ReturnTtl,
            ]
        );
    }

    #[test]
    fn temporal_instructions_reject_more_than_three_entries() {
        let mut instructions = ArithmeticTemporalInstructions::default();
        for instruction in [
            ArithmeticTemporalInstruction::Vivify(30),
            ArithmeticTemporalInstruction::UpdateTtl(60),
            ArithmeticTemporalInstruction::ReturnTtl,
        ] {
            instructions.push(instruction).unwrap();
        }

        assert_eq!(
            instructions.push(ArithmeticTemporalInstruction::ReturnTtl),
            Err(ParseError::TooManyFlags)
        );
    }
}
