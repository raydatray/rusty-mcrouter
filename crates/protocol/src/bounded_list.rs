#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundedList<T, const N: usize> {
    items: [Option<T>; N],
    len: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapacityExceeded;

impl<T, const N: usize> Default for BoundedList<T, N> {
    fn default() -> Self {
        Self {
            items: std::array::from_fn(|_| None),
            len: 0,
        }
    }
}

impl<T, const N: usize> BoundedList<T, N> {
    pub(crate) fn push(&mut self, item: T) -> Result<(), CapacityExceeded> {
        let slot = self
            .items
            .get_mut(usize::from(self.len))
            .ok_or(CapacityExceeded)?;
        *slot = Some(item);
        self.len += 1;
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items[..usize::from(self.len)].iter().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_insertion_order() {
        let mut list = BoundedList::<u32, 4>::default();
        for item in [3, 1, 2] {
            list.push(item).unwrap();
        }
        assert_eq!(list.iter().copied().collect::<Vec<_>>(), vec![3, 1, 2]);
    }

    #[test]
    fn rejects_pushes_past_capacity_and_stays_intact() {
        let mut list = BoundedList::<u32, 2>::default();
        list.push(1).unwrap();
        list.push(2).unwrap();
        assert_eq!(list.push(3), Err(CapacityExceeded));
        assert_eq!(list.iter().copied().collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn empty_list_iterates_nothing() {
        let list = BoundedList::<u32, 4>::default();
        assert_eq!(list.iter().count(), 0);
    }
}
