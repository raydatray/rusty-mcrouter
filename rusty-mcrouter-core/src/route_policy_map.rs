use std::{collections::BTreeSet, rc::Rc};

use crate::{
    lower_bound_prefix_map::{LowerBoundPrefixMap, PrefixValue},
    prefix_selector::PrefixSelector,
    DynRoute,
};

pub(crate) struct RoutePolicyMap {
    targets: LowerBoundPrefixMap<Vec<Rc<dyn DynRoute>>>,
}

impl RoutePolicyMap {
    pub(crate) fn new(selectors: &[Rc<PrefixSelector>]) -> Self {
        let mut prefixes = selectors
            .iter()
            .flat_map(|selector| selector.policy_prefixes().map(<[u8]>::to_vec))
            .collect::<BTreeSet<_>>();

        prefixes.insert(vec![]);

        let entries = prefixes
            .into_iter()
            .map(|prefix| {
                let targets = select_unique_targets(selectors, &prefix);
                PrefixValue::new(prefix, targets)
            })
            .collect();

        Self {
            targets: LowerBoundPrefixMap::new(entries),
        }
    }

    pub(crate) fn targets(&self, key: &[u8]) -> &[Rc<dyn DynRoute>] {
        self.targets
            .find_prefix(key)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}

fn select_unique_targets(selectors: &[Rc<PrefixSelector>], key: &[u8]) -> Vec<Rc<dyn DynRoute>> {
    let mut targets = Vec::new();

    for selector in selectors {
        let Some(route) = selector.select(key) else {
            continue;
        };

        let already_selected = targets.iter().any(|target| Rc::ptr_eq(target, &route));

        if !already_selected {
            targets.push(route);
        }
    }

    targets
}
