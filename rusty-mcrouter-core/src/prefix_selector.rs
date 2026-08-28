use std::rc::Rc;

use crate::{
    lower_bound_prefix_map::{LowerBoundPrefixMap, PrefixValue},
    DynRoute,
};

pub(crate) struct PrefixPolicy {
    prefix: Vec<u8>,
    route: Rc<dyn DynRoute>,
}

impl PrefixPolicy {
    pub(crate) fn new(prefix: Vec<u8>, route: Rc<dyn DynRoute>) -> Self {
        Self { prefix, route }
    }
}

pub(crate) struct PrefixSelector {
    policies: LowerBoundPrefixMap<Rc<dyn DynRoute>>,
    wildcard: Option<Rc<dyn DynRoute>>,
}

impl PrefixSelector {
    pub(crate) fn new(policies: Vec<PrefixPolicy>, wildcard: Option<Rc<dyn DynRoute>>) -> Self {
        let policies = policies
            .into_iter()
            .map(|policy| PrefixValue::new(policy.prefix, policy.route))
            .collect();

        Self {
            policies: LowerBoundPrefixMap::new(policies),
            wildcard,
        }
    }

    pub(crate) fn select(&self, key: &[u8]) -> Option<Rc<dyn DynRoute>> {
        self.policies
            .find_prefix(key)
            .cloned()
            .or_else(|| self.wildcard.clone())
    }

    pub(crate) fn policy_prefixes(&self) -> impl Iterator<Item = &[u8]> {
        self.policies.keys()
    }
}
